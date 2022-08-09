use lunatic::process::Message;
use lunatic::process::Request;
use lunatic::sleep;
use lunatic::{process::StartProcess, Mailbox};
use lunatic_log::debug;
use lunatic_log::{info, subscriber::fmt::FmtSubscriber, LevelFilter};
use munnel::consumer::AskDemandMessage;
use munnel::consumer::Consumer;
use munnel::consumer::SubscribeToMessage;
use munnel::Ping;
use munnel::ProducerStage;
use munnel::{DemandMessage, Producer, SubscribeMessage};
use std::ops::Add;
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
struct PStage(usize);
impl ProducerStage for PStage {
    type Output = usize;

    fn handle_demand(&mut self, new_demand: munnel::DemandCount) -> Vec<usize> {
        // artificicially return more than demand
        let new_demand = new_demand + 20;

        let events = (self.0..new_demand.into()).collect();
        self.0 = self.0 + new_demand;

        events
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
struct CStage(usize);

impl CStage {
    fn handle_events(&mut self, events: Vec<usize>) {
        self.0 += events.into_iter().sum::<usize>();
    }
}

#[lunatic::main]
fn main(_: Mailbox<()>) {
    lunatic_log::init(FmtSubscriber::new(LevelFilter::Debug).pretty());
    let two_seconds = Duration::from_secs(1);

    let producer = Producer::start_link(PStage::default(), None);
    let (con1, con2, con3, con4) = (
        Consumer::<usize, CStage>::start_link_with_handler(CStage::handle_events),
        Consumer::<usize, CStage>::start_link_with_handler(CStage::handle_events),
        Consumer::<usize, CStage>::start_link_with_handler(CStage::handle_events),
        Consumer::<usize, CStage>::start_link_with_handler(CStage::handle_events),
    );

    let _ = con1.request(SubscribeToMessage(producer.clone()));
    let _ = con2.request(SubscribeToMessage(producer.clone()));
    let _ = con3.request(SubscribeToMessage(producer.clone()));
    let _ = con4.request(SubscribeToMessage(producer.clone()));

    con1.send(AskDemandMessage(producer.clone(), 100));
    con2.send(AskDemandMessage(producer.clone(), 110));
    con3.send(AskDemandMessage(producer.clone(), 80));
    con4.send(AskDemandMessage(producer.clone(), 80));

    sleep(two_seconds);
    let _ = con1.shutdown_timeout(two_seconds);
    let _ = con2.shutdown_timeout(two_seconds);
    let _ = con3.shutdown_timeout(two_seconds);
    let _ = con4.shutdown_timeout(two_seconds);
    sleep(two_seconds);
}
