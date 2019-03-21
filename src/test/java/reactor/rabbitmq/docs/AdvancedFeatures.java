/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq.docs;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;
import reactor.rabbitmq.Utils;

/**
 *
 */
@SuppressWarnings("unused")
public class AdvancedFeatures {

    void connectionMono() {
        // tag::connection-mono[]
        ConnectionFactory connectionFactory = new ConnectionFactory();                // <1>
        connectionFactory.useNio();

        Sender sender = RabbitFlux.createSender(new SenderOptions()
            .connectionMono(
                Mono.fromCallable(() -> connectionFactory.newConnection("sender")))   // <2>
        );
        Receiver receiver = RabbitFlux.createReceiver(new ReceiverOptions()
            .connectionMono(
                Mono.fromCallable(() -> connectionFactory.newConnection("receiver"))) // <3>
        );
        // end::connection-mono[]
    }

    void sharedConnection() {
        // tag::shared-connection[]
        ConnectionFactory connectionFactory = new ConnectionFactory();           // <1>
        connectionFactory.useNio();
        Mono<? extends Connection> connectionMono = Utils.singleConnectionMono(  // <2>
            connectionFactory, cf -> cf.newConnection()
        );

        Sender sender = RabbitFlux.createSender(
            new SenderOptions().connectionMono(connectionMono)                   // <3>
        );
        Receiver receiver = RabbitFlux.createReceiver(
            new ReceiverOptions().connectionMono(connectionMono)                 // <4>
        );
        // end::shared-connection[]
    }

}
