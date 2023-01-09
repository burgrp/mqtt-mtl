const mqtt = require("mqtt");

let brokers = {};

module.exports = brokerUrl => {

    if (!brokers[brokerUrl]) {
        let broker = {

            connection: mqtt.connect(brokerUrl, {
                protocolVersion: 5
            }),

            listeners: {},
            nextListenerId: 1,

            publish(topic, message) {
                this.connection.publish(topic, message);
            },

            subscribe(topic, listener) {

                let id = this.nextListenerId++;

                this.listeners[id] = listener;

                this.connection.subscribe(topic, {
                    properties: {
                        subscriptionIdentifier: id
                    }
                });

                return id;
            },

            unsubscribe(listenerId) {
                //TODO: unsubscribe from MQTT if it's last subscription in this.subscriptions[]
                //delete this.listeners[listenerId];
                throw new Error("Not implemented yet");
            }
        }

        brokers[brokerUrl] = broker;

        broker.connection.on("message", (topic, message, packet) => {

            let ids = (packet.properties && packet.properties.subscriptionIdentifier) || [];
            if (!(ids instanceof Array)) {
                ids = [ids];
            }

            for (let id of ids) {
                let listener = broker.listeners[id];
                if (listener) {
                    try {
                        listener(topic, message);
                    } catch (e) {
                        console.error("Unhandled MQTT listener exception:", e);
                    }
                }
            }

        });
    }

    return brokers[brokerUrl];
}