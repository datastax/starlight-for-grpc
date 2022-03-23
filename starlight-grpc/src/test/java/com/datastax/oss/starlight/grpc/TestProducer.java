package com.datastax.oss.starlight.grpc;

import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_METADATA_KEY;
import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ProducerAck;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerSend;
import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.Base64;
import org.slf4j.LoggerFactory;

import java.time.Instant;


public class TestProducer {

  public static void main(String[] args) throws Exception {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
        .usePlaintext()
        .build();

    Metadata headers = new Metadata();
    byte[] params = ClientParameters.newBuilder().setTopic("test-topic").build().toByteArray();
    headers.put(CLIENT_PARAMS_METADATA_KEY, params);
    PulsarGrpc.PulsarStub asyncStub = MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel), headers);

    StreamObserver<ProducerRequest> producer = asyncStub.produce(new StreamObserver<ProducerResponse>() {
      @Override
      public void onNext(ProducerResponse response) {
        if (response.hasAck()) {
          ProducerAck value = response.getAck();
          String msgId = Base64.getEncoder().encodeToString(value.getMessageId().toByteArray());
          System.out.println("producer ack received: " + msgId + " " + value.getContext() + " "  + Instant.now());
        }
      }

      @Override
      public void onError(Throwable t) {
        LoggerFactory.getLogger("foo").error(t.getMessage());
      }

      @Override
      public void onCompleted() {
        LoggerFactory.getLogger("foo").error("completed");
      }
    });

    for(int i=0; i < 10; i++) {
      producer.onNext(ProducerRequest.newBuilder().setSend(ProducerSend.newBuilder()
          .setPayload(ByteString.copyFromUtf8("test" + i))
          .setContext("" + i))
          .build());
    }

    while(true) {
      Thread.sleep(1);
    }
  }
}
