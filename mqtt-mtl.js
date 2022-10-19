const mqtt = require("mqtt");

let brokers = {};

module.exports = brokerUrl => {

    if (!brokers[brokerUrl]) {
        let broker = {

            connection: mqtt.connect(brokerUrl),

            listeners: [],
            nextListenerId: 0,

            publish(topic, message) {
                this.connection.publish(topic, message);
            },

            subscribe(topic, listener) {
                this.connection.subscribe(topic);

                let parsedTopic = topic.split("/");

                let id = this.nextListenerId++;

                this.listeners[id] = (actTopic, actMessage) => {
                    let parsedActTopic = actTopic.split("/");
                    let matches = true;
                    for (let i in parsedTopic) {
                        if (parsedTopic[i] === parsedActTopic[i] || parsedTopic[i] === "+") {
                            continue;
                        }
                        if (parsedTopic[i] === "#") {
                            break;
                        }
                        matches = false;
                        break;
                    }
                    if (matches) {
                        listener(actTopic, actMessage);
                    }
                };

                return id;
            },

            unsubscribe(listenerId) {
                delete this.listeners[listenerId];
            }
        }

        brokers[brokerUrl] = broker;

        broker.connection.on("message", (topic, message) => {
            for (listener of Object.values(broker.listeners)) {
                try {
                    listener(topic, message);
                } catch(e) {
                    console.error("Unhandled MQTT listener exception:", e);
                }
            }
        });
    }

    return brokers[brokerUrl];
}