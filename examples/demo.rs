use std::time::Duration;

use lunatic::process::Message;
use lunatic::process::Request;
use lunatic::{process::StartProcess, Mailbox};
use munnel::consumer::Consumer;
use munnel::Ping;
use munnel::ProducerStage;
use munnel::{AskDemandMessage, Producer, SubscribeMessage};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct PStage;
impl ProducerStage for PStage {}

#[lunatic::main]
fn main(_: Mailbox<()>) {
    let producer = Producer::start_link(PStage, None);
    let (con1, con2, con3, con4) = (
        Consumer::start_link((), None),
        Consumer::start_link((), None),
        Consumer::start_link((), None),
        Consumer::start_link((), None),
    );

    producer.send(SubscribeMessage(con1.clone()));
    producer.send(SubscribeMessage(con2.clone()));
    producer.send(SubscribeMessage(con3.clone()));

    let _ = producer.request(AskDemandMessage(con1, 100));
    let _ = producer.request(AskDemandMessage(con2, 110));
    let _ = producer.request(AskDemandMessage(con3, 80));
    let _ = producer.request(AskDemandMessage(con4, 80));

    producer.request(Ping);
}
