/*
 * (C) Copyright 2015-2016 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kurento.tutorial.helloworld;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kurento.client.AudioCaps;
import org.kurento.client.AudioCodec;
import org.kurento.client.EndOfStreamEvent;
import org.kurento.client.ErrorEvent;
import org.kurento.client.EventListener;
import org.kurento.client.IceCandidate;
import org.kurento.client.IceCandidateFoundEvent;
import org.kurento.client.KurentoClient;
import org.kurento.client.MediaPipeline;
import org.kurento.client.MediaProfileSpecType;
import org.kurento.client.MediaType;
import org.kurento.client.PausedEvent;
import org.kurento.client.PlayerEndpoint;
import org.kurento.client.RecorderEndpoint;
import org.kurento.client.RecordingEvent;
import org.kurento.client.StoppedEvent;
import org.kurento.client.WebRtcEndpoint;
import org.kurento.jsonrpc.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.speech.v1beta1.RecognitionConfig;
import com.google.cloud.speech.v1beta1.RecognitionConfig.AudioEncoding;
import com.google.cloud.speech.v1beta1.SpeechGrpc;
import com.google.cloud.speech.v1beta1.StreamingRecognitionConfig;
import com.google.cloud.speech.v1beta1.StreamingRecognizeRequest;
import com.google.cloud.speech.v1beta1.StreamingRecognizeResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.stub.StreamObserver;

/**
 * Hello World with recording handler (application and media logic).
 *
 * @author Boni Garcia (bgarcia@gsyc.es)
 * @author David Fernandez (d.fernandezlop@gmail.com)
 * @author Radu Tom Vlad (rvlad@naevatec.com)
 * @author Ivan Gracia (igracia@kurento.org)
 * @since 6.1.1
 */
public class HelloWorldRecHandler extends TextWebSocketHandler {

	private static final int SAMPLE_RATE = 16000;
	private static final String RECORDER_FILE_PATH = "file:///opt/recorder/HelloWorldRecorded.webm";
	private static final String LOCAL_RECORDER_FILE_PATH = "/home/patxi/tmp/HelloWorldRecorded.webm";
	private static final int BYTES_PER_BUFFER = 3200; // buffer size in bytes
	private static final int BYTES_PER_SAMPLE = 2; // bytes per sample for
													// LINEAR16
	private static final List<String> OAUTH2_SCOPES = Arrays.asList("https://www.googleapis.com/auth/cloud-platform");

	private final Logger log = LoggerFactory.getLogger(HelloWorldRecHandler.class);
	private static final Gson gson = new GsonBuilder().create();

	@Autowired
	private UserRegistry registry;

	@Autowired
	private KurentoClient kurento;
	private ManagedChannel channel;
	private volatile boolean inProgess;

	@Override
	public void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		JsonObject jsonMessage = gson.fromJson(message.getPayload(), JsonObject.class);

		log.debug("Incoming message: {}", jsonMessage);

		UserSession user = registry.getBySession(session);
		if (user != null) {
			log.debug("Incoming message from user '{}': {}", user.getId(), jsonMessage);
		} else {
			log.debug("Incoming message from new user: {}", jsonMessage);
		}

		switch (jsonMessage.get("id").getAsString()) {
		case "start":
			start(session, jsonMessage);
			break;
		case "stop":
			if (user != null) {
				channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
				user.stop();
			}
		case "stopPlay":
			if (user != null) {
				inProgess = false;
				user.release();
			}
			break;
		case "play":
			play(user, session, jsonMessage);
			break;
		case "onIceCandidate": {
			JsonObject jsonCandidate = jsonMessage.get("candidate").getAsJsonObject();

			if (user != null) {
				IceCandidate candidate = new IceCandidate(jsonCandidate.get("candidate").getAsString(),
						jsonCandidate.get("sdpMid").getAsString(), jsonCandidate.get("sdpMLineIndex").getAsInt());
				user.addCandidate(candidate);
			}
			break;
		}
		default:
			sendError(session, "Invalid message with id " + jsonMessage.get("id").getAsString());
			break;
		}
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		super.afterConnectionClosed(session, status);
		registry.removeBySession(session);
	}

	private void start(final WebSocketSession session, JsonObject jsonMessage) {
		try {

			// 1. Media logic (webRtcEndpoint in loopback)
			MediaPipeline pipeline = kurento.createMediaPipeline();
			WebRtcEndpoint webRtcEndpoint = new WebRtcEndpoint.Builder(pipeline).build();
			webRtcEndpoint.connect(webRtcEndpoint);

			MediaProfileSpecType profile = getMediaProfileFromMessage(jsonMessage);

			RecorderEndpoint recorder = new RecorderEndpoint.Builder(pipeline, RECORDER_FILE_PATH)
					.withMediaProfile(MediaProfileSpecType.WEBM_AUDIO_ONLY).build();
			recorder.setAudioFormat(new AudioCaps(AudioCodec.RAW, 16000));

			recorder.addRecordingListener(new EventListener<RecordingEvent>() {

				@Override
				public void onEvent(RecordingEvent event) {
					JsonObject response = new JsonObject();
					response.addProperty("id", "recording");
					try {
						synchronized (session) {
							session.sendMessage(new TextMessage(response.toString()));
						}
					} catch (IOException e) {
						log.error(e.getMessage());
					}
				}

			});

			recorder.addStoppedListener(new EventListener<StoppedEvent>() {

				@Override
				public void onEvent(StoppedEvent event) {
					JsonObject response = new JsonObject();
					response.addProperty("id", "stopped");
					try {
						synchronized (session) {
							session.sendMessage(new TextMessage(response.toString()));
						}
					} catch (IOException e) {
						log.error(e.getMessage());
					}
				}

			});

			recorder.addPausedListener(new EventListener<PausedEvent>() {

				@Override
				public void onEvent(PausedEvent event) {
					JsonObject response = new JsonObject();
					response.addProperty("id", "paused");
					try {
						synchronized (session) {
							session.sendMessage(new TextMessage(response.toString()));
						}
					} catch (IOException e) {
						log.error(e.getMessage());
					}
				}

			});

			connectAccordingToProfile(webRtcEndpoint, recorder, profile);

			// 2. Store user session
			UserSession user = new UserSession(session);
			user.setMediaPipeline(pipeline);
			user.setWebRtcEndpoint(webRtcEndpoint);
			user.setRecorderEndpoint(recorder);
			registry.register(user);

			// 3. SDP negotiation
			String sdpOffer = jsonMessage.get("sdpOffer").getAsString();
			String sdpAnswer = webRtcEndpoint.processOffer(sdpOffer);

			// 4. Gather ICE candidates
			webRtcEndpoint.addIceCandidateFoundListener(new EventListener<IceCandidateFoundEvent>() {

				@Override
				public void onEvent(IceCandidateFoundEvent event) {
					JsonObject response = new JsonObject();
					response.addProperty("id", "iceCandidate");
					response.add("candidate", JsonUtils.toJsonObject(event.getCandidate()));
					try {
						synchronized (session) {
							session.sendMessage(new TextMessage(response.toString()));
						}
					} catch (IOException e) {
						log.error(e.getMessage());
					}
				}
			});

			JsonObject response = new JsonObject();
			response.addProperty("id", "startResponse");
			response.addProperty("sdpAnswer", sdpAnswer);

			synchronized (user) {
				session.sendMessage(new TextMessage(response.toString()));
			}

			webRtcEndpoint.gatherCandidates();

			recorder.record();
			inProgess = true;

			speechToTextStart();

		} catch (Throwable t) {
			log.error("Start error", t);
			sendError(session, t.getMessage());
		}
	}

	@Async
	private void speechToTextStart() throws InterruptedException {

		try {
			channel = createChannel("speech.googleapis.com", 443);
		} catch (Exception e) {
			log.error("Failed to create channel", e);
		}

		SpeechGrpc.SpeechStub speechClient = SpeechGrpc.newStub(channel);

		final CountDownLatch finishLatch = new CountDownLatch(1);
		StreamObserver<StreamingRecognizeResponse> responseObserver = new StreamObserver<StreamingRecognizeResponse>() {
			@Override
			public void onNext(StreamingRecognizeResponse response) {
				log.info("Received response: " + TextFormat.printToString(response));
			}

			@Override
			public void onError(Throwable error) {
				log.warn("recognize failed: {0}", error);
				finishLatch.countDown();
			}

			@Override
			public void onCompleted() {
				log.info("recognize completed.");
				finishLatch.countDown();
			}
		};

		StreamObserver<StreamingRecognizeRequest> requestObserver;
		try {
			requestObserver = speechClient.streamingRecognize(responseObserver);
		} catch(Exception e) {
			log.error("Speech client:couldn't configure streaming recognize with response observer");
			throw e;
		}
		
		try {
			// Build and send a StreamingRecognizeRequest containing the
			// parameters for
			// processing the audio.
			RecognitionConfig config = RecognitionConfig.newBuilder().setEncoding(AudioEncoding.LINEAR16)
					.setSampleRate(SAMPLE_RATE).build();
			StreamingRecognitionConfig streamingConfig = StreamingRecognitionConfig.newBuilder().setConfig(config)
					.setInterimResults(true).setSingleUtterance(true).build();

			StreamingRecognizeRequest initial = StreamingRecognizeRequest.newBuilder()
					.setStreamingConfig(streamingConfig).build();
			requestObserver.onNext(initial);

			// Open audio file. Read and send sequential buffers of audio as
			// additional RecognizeRequests.

			try (FileInputStream in = new FileInputStream(new File(LOCAL_RECORDER_FILE_PATH))) {
				// For LINEAR16 at 16000 Hz sample rate, 3200 bytes corresponds
				// to 100 milliseconds of audio.
				byte[] buffer = new byte[BYTES_PER_BUFFER];
				int bytesRead;
				int totalBytes = 0;
				int samplesPerBuffer = BYTES_PER_BUFFER / BYTES_PER_SAMPLE;
				int samplesPerMillis = 16_000 / 1000;

				while (inProgess) {
					while ((bytesRead = in.read(buffer)) != -1) {
						totalBytes += bytesRead;
						StreamingRecognizeRequest request = StreamingRecognizeRequest.newBuilder()
								.setAudioContent(ByteString.copyFrom(buffer, 0, bytesRead)).build();
						requestObserver.onNext(request);
					}
				}
				log.info("Sent " + totalBytes + " bytes from audio file: " + LOCAL_RECORDER_FILE_PATH);

			} catch (FileNotFoundException e) {
				log.error("File not found: " + e.getMessage());
			} catch (IOException e) {
				log.error("Error reading file: " + e.getMessage());
			}
		} catch (RuntimeException e) {
			// Cancel RPC.
			log.error("Runtime error: " + e.getMessage()); 
			requestObserver.onError(e);
			throw e;
		}
		// Mark the end of requests.
		requestObserver.onCompleted();

		// Receiving happens asynchronously.
		finishLatch.await(1, TimeUnit.MINUTES);
	}

	private ManagedChannel createChannel(String host, int port) throws IOException {
		GoogleCredentials creds = GoogleCredentials.getApplicationDefault();
		creds = creds.createScoped(OAUTH2_SCOPES);
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
				.intercept(new ClientAuthInterceptor(creds, Executors.newSingleThreadExecutor())).build();

		return channel;
	}

	private MediaProfileSpecType getMediaProfileFromMessage(JsonObject jsonMessage) {

		// MediaProfileSpecType profile;
		// switch (jsonMessage.get("mode").getAsString()) {
		// case "audio-only":
		// profile = MediaProfileSpecType.WEBM_AUDIO_ONLY;
		// break;
		// case "video-only":
		// profile = MediaProfileSpecType.WEBM_VIDEO_ONLY;
		// break;
		// default:
		// profile = MediaProfileSpecType.WEBM;
		// }
		//
		// return profile;
		return MediaProfileSpecType.WEBM_AUDIO_ONLY;
	}

	private void connectAccordingToProfile(WebRtcEndpoint webRtcEndpoint, RecorderEndpoint recorder,
			MediaProfileSpecType profile) {
		switch (profile) {
		case WEBM:
			webRtcEndpoint.connect(recorder, MediaType.AUDIO);
			webRtcEndpoint.connect(recorder, MediaType.VIDEO);
			break;
		case WEBM_AUDIO_ONLY:
			webRtcEndpoint.connect(recorder, MediaType.AUDIO);
			break;
		case WEBM_VIDEO_ONLY:
			webRtcEndpoint.connect(recorder, MediaType.VIDEO);
			break;
		default:
			throw new UnsupportedOperationException("Unsupported profile for this tutorial: " + profile);
		}
	}

	private void play(UserSession user, final WebSocketSession session, JsonObject jsonMessage) {
		try {

			// 1. Media logic
			final MediaPipeline pipeline = kurento.createMediaPipeline();
			WebRtcEndpoint webRtcEndpoint = new WebRtcEndpoint.Builder(pipeline).build();
			PlayerEndpoint player = new PlayerEndpoint.Builder(pipeline, RECORDER_FILE_PATH).build();
			player.connect(webRtcEndpoint);

			// Player listeners
			player.addErrorListener(new EventListener<ErrorEvent>() {
				@Override
				public void onEvent(ErrorEvent event) {
					log.info("ErrorEvent for session '{}': {}", session.getId(), event.getDescription());
					sendPlayEnd(session, pipeline);
				}
			});
			player.addEndOfStreamListener(new EventListener<EndOfStreamEvent>() {
				@Override
				public void onEvent(EndOfStreamEvent event) {
					log.info("EndOfStreamEvent for session '{}'", session.getId());
					sendPlayEnd(session, pipeline);
				}
			});

			// 2. Store user session
			user.setMediaPipeline(pipeline);
			user.setWebRtcEndpoint(webRtcEndpoint);

			// 3. SDP negotiation
			String sdpOffer = jsonMessage.get("sdpOffer").getAsString();
			String sdpAnswer = webRtcEndpoint.processOffer(sdpOffer);

			JsonObject response = new JsonObject();
			response.addProperty("id", "playResponse");
			response.addProperty("sdpAnswer", sdpAnswer);

			// 4. Gather ICE candidates
			webRtcEndpoint.addIceCandidateFoundListener(new EventListener<IceCandidateFoundEvent>() {

				@Override
				public void onEvent(IceCandidateFoundEvent event) {
					JsonObject response = new JsonObject();
					response.addProperty("id", "iceCandidate");
					response.add("candidate", JsonUtils.toJsonObject(event.getCandidate()));
					try {
						synchronized (session) {
							session.sendMessage(new TextMessage(response.toString()));
						}
					} catch (IOException e) {
						log.error(e.getMessage());
					}
				}
			});

			// 5. Play recorded stream
			player.play();

			synchronized (session) {
				session.sendMessage(new TextMessage(response.toString()));
			}

			webRtcEndpoint.gatherCandidates();
		} catch (Throwable t) {
			log.error("Play error", t);
			sendError(session, t.getMessage());
		}
	}

	public void sendPlayEnd(WebSocketSession session, MediaPipeline pipeline) {
		try {
			JsonObject response = new JsonObject();
			response.addProperty("id", "playEnd");
			session.sendMessage(new TextMessage(response.toString()));
		} catch (IOException e) {
			log.error("Error sending playEndOfStream message", e);
		}
		// Release pipeline
		pipeline.release();
	}

	private void sendError(WebSocketSession session, String message) {
		try {
			JsonObject response = new JsonObject();
			response.addProperty("id", "error");
			response.addProperty("message", message);
			session.sendMessage(new TextMessage(response.toString()));
		} catch (IOException e) {
			log.error("Exception sending message", e);
		}
	}
}
