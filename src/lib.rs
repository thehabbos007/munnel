use std::collections::LinkedList;
use std::fmt::Display;
use std::time::Duration;
use std::{
    collections::HashSet,
    hash::{self, Hash},
};

use consumer::Consumer;
use lunatic::process::{MessageHandler, RequestHandler};
use lunatic::{
    abstract_process,
    process::{AbstractProcess, Message, ProcessRef},
};
use serde::{Deserialize, Serialize};

pub mod consumer {
    use lunatic::{
        abstract_process,
        process::{MessageHandler, ProcessRef},
    };
    use std::time::Duration;

    #[derive(PartialEq, Eq, Hash)]
    pub struct Consumer;

    #[abstract_process]
    impl Consumer {
        #[init]
        fn init(_: ProcessRef<Self>, _: ()) -> Self {
            Self
        }
    }

    impl MessageHandler<()> for Consumer {
        fn handle(_state: &mut Self::State, _message: ()) {
            ()
        }
    }
}

// Notes: A demand is a PID and a monitor ref (the subscription tag) along with a "counter"
// When requesting demand, the "demand" is queued up
// When dispatching events, the first demand is popped and sent off throuhg its ref
// and the consuemr always has the pid of the producer as long as its monitor ref

/*
Popdemand works with subscription tags, and will always pop based on the given ref
    https://github.com/elixir-lang/gen_stage/blob/main/lib/gen_stage/dispatchers/demand_dispatcher.ex#L118

Ask requires subscribe? It's like an update it seems
    https://github.com/elixir-lang/gen_stage/blob/main/lib/gen_stage/dispatchers/demand_dispatcher.ex#L64
*/

/*

*/
/*
when sending/dispatching events returning the now, later,, length of events, counter;

length <= counter ->
    send all events to the demand
    {now, [], length - counter, 0}



length > counter ->
    split in "now" and "later".
    {now, later, length - counter, 0}


For either:
    Send now demand
    Add demand to demand list
    dispatch next demand

*/

#[derive(Debug)]
struct Demand {
    demand_count: usize,
    process: ProcessRef<Consumer>,
}

pub trait ProducerStage: std::fmt::Debug + Serialize + for<'de> Deserialize<'de> {}

#[derive(Debug)]
pub struct Producer<S: ProducerStage> {
    demands: LinkedList<Demand>,
    max_demand: Option<usize>,
    consumers: HashSet<ProcessRef<Consumer>>,
    stage: S,
}

impl<S: ProducerStage> AbstractProcess for Producer<S> {
    type Arg = S;

    type State = Self;

    fn init(_: ProcessRef<Self>, stage: Self::Arg) -> Self::State {
        Self {
            demands: LinkedList::new(),
            max_demand: None,
            consumers: HashSet::new(),
            stage,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubscribeMessage(pub ProcessRef<Consumer>);

impl<S: ProducerStage> MessageHandler<SubscribeMessage> for Producer<S> {
    fn handle(state: &mut Self::State, SubscribeMessage(consumer): SubscribeMessage) {
        state.consumers.insert(consumer);
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AskDemandMessage(pub ProcessRef<Consumer>, pub usize);

#[derive(Serialize, Deserialize, Debug)]
pub struct ExceededMaxDemand;

/*
When adding demand. Counter is current "target new items to consume", c is first item in demand list
Always have largest demand first.

And there's a max demand based on the first demand's counter

When counter > c ->
    Add demand in the start

(counter <= c) Otherwise ->
    Add demand as 2nd entry

*/

impl<S: ProducerStage> RequestHandler<AskDemandMessage> for Producer<S> {
    type Response = Result<(), ExceededMaxDemand>;

    fn handle(
        state: &mut Self::State,
        AskDemandMessage(consumer, demand_count): AskDemandMessage,
    ) -> Self::Response {
        let max_demand = state.max_demand.get_or_insert(demand_count);
        if demand_count > *max_demand {
            return Err(ExceededMaxDemand);
        }

        let new_demand = Demand {
            demand_count,
            process: consumer,
        };
        // Insert demand as second item if counter > first.counter
        if let Some(first) = state.demands.pop_front() {
            if demand_count > first.demand_count {
                state.demands.push_front(first);
                state.demands.push_front(new_demand);
            } else {
                state.demands.push_front(new_demand);
                state.demands.push_front(first);
            }
        } else {
            state.demands.push_front(new_demand);
        }

        dbg!(&state);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
