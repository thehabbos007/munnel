use std::collections::HashSet;
use std::collections::LinkedList;

use lunatic::process::{AbstractProcess, ProcessRef};
use lunatic::process::{MessageHandler, RequestHandler};
use serde::{Deserialize, Serialize};

use consumer::Consumer;
pub mod consumer {
    use std::marker::PhantomData;

    use lunatic::{
        abstract_process,
        process::{AbstractProcess, MessageHandler, ProcessRef},
    };

    use serde::{Deserialize, Serialize};

    use crate::ProducerStage;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
    pub struct Consumer<I> {
        _phantom_data: PhantomData<I>,
    }

    pub trait ConsumerStage<I>: std::fmt::Debug + Serialize + for<'de> Deserialize<'de> {
        fn handle_events(&mut self, events: Vec<I>);
    }
    pub struct ConsumerState<I> {
        _phantom_data: PhantomData<I>,
    }

    impl<I> AbstractProcess for Consumer<I> {
        type Arg = ();

        type State = ConsumerState<I>;

        fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
            Self::State {
                _phantom_data: PhantomData,
            }
        }
    }

    impl<I> MessageHandler<()> for Consumer<I> {
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

pub type DemandCount = usize;

pub trait ProducerStage: std::fmt::Debug + Serialize + for<'de> Deserialize<'de> {
    type Output: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug;

    fn handle_demand(&mut self, demand: DemandCount) -> Vec<Self::Output>;
}

#[derive(Debug)]
struct Demand<I> {
    demand_count: DemandCount,
    process: ProcessRef<Consumer<I>>,
}

#[derive(Debug)]
pub struct Producer<S: ProducerStage> {
    demands: LinkedList<Demand<S::Output>>,
    max_demand: Option<DemandCount>,
    consumers: HashSet<ProcessRef<Consumer<S::Output>>>,
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
pub struct SubscribeMessage<I>(pub ProcessRef<Consumer<I>>);

impl<S: ProducerStage> MessageHandler<SubscribeMessage<S::Output>> for Producer<S> {
    fn handle(state: &mut Self::State, SubscribeMessage(consumer): SubscribeMessage<S::Output>) {
        state.consumers.insert(consumer);
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AskDemandMessage<I>(pub ProcessRef<Consumer<I>>, pub DemandCount);

#[derive(Serialize, Deserialize, Debug)]
pub enum DemandResponse {
    ExceededMaxDemand,
    DontKnowYou,
}

/*
When adding demand. Counter is current "target new items to consume", c is first item in demand list
Always have largest demand first.

And there's a max demand based on the first demand's counter

When counter > c ->
    Add demand in the start

(counter <= c) Otherwise ->
    Add demand as 2nd entry

*/

impl<S: ProducerStage> RequestHandler<AskDemandMessage<S::Output>> for Producer<S> {
    type Response = Result<(), DemandResponse>;

    fn handle(
        state: &mut Self::State,
        AskDemandMessage(consumer, demand_count): AskDemandMessage<S::Output>,
    ) -> Self::Response {
        if !state.consumers.contains(&consumer) {
            return Err(DemandResponse::DontKnowYou);
        }

        let max_demand = state.max_demand.get_or_insert(demand_count);
        if demand_count > *max_demand {
            return Err(DemandResponse::ExceededMaxDemand);
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

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Ping;

impl<S: ProducerStage> RequestHandler<Ping> for Producer<S> {
    type Response = ();
    fn handle(state: &mut Self::State, _: Ping) {
        dbg!(&state);
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
