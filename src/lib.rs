use std::collections::HashSet;
use std::collections::LinkedList;
use std::fmt::Debug;

use itertools::Itertools;
use lunatic::process::Message;
use lunatic::process::{AbstractProcess, ProcessRef};
use lunatic::process::{MessageHandler, RequestHandler};
use lunatic_log::debug;
use lunatic_log::info;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use consumer::Consumer;
pub mod consumer {
    use std::fmt::Debug;
    use std::{marker::PhantomData, mem};

    use lunatic::process::Message;
    use lunatic::process::Request;
    use lunatic::process::RequestHandler;

    use lunatic::process::{AbstractProcess, MessageHandler, ProcessRef, StartProcess};

    use lunatic_log::debug;
    use lunatic_log::info;
    use serde::{de::DeserializeOwned, Deserialize, Serialize};

    use crate::Ask;
    use crate::DemandCount;
    use crate::DemandMessage;
    use crate::DemandResponse;
    use crate::Producer;
    use crate::ProducerStage;
    use crate::SubscribeMessage;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
    #[serde(bound = "")]
    pub struct Consumer<I, C>
    where
        C: Default + Serialize + DeserializeOwned,
    {
        demand: usize,
        state: C,
        _phantom_data: PhantomData<I>,
    }

    impl<I, C> Consumer<I, C>
    where
        C: Default + Serialize + DeserializeOwned,
    {
        fn with_optional_state(state: Option<C>) -> Self {
            Self {
                demand: 100,
                state: state.unwrap_or_default(),
                _phantom_data: PhantomData,
            }
        }
    }

    pub trait ConsumerStage<I>: Debug {
        fn handle_event(&mut self, event: Vec<I>);
    }

    pub struct Stage<I, C>
    where
        C: Default + Serialize + DeserializeOwned,
    {
        this: ProcessRef<Consumer<I, C>>,
        stage_fn: fn(&mut C, Vec<I>) -> (),
        state: Consumer<I, C>,
    }

    impl<I, C> Stage<I, C>
    where
        I: Serialize + DeserializeOwned,
        C: Default + Serialize + DeserializeOwned,
    {
        fn inner_state(&mut self) -> &mut C {
            &mut self.state.state
        }
    }

    impl<I, C> AbstractProcess for Consumer<I, C>
    where
        C: Debug + Default + Serialize + DeserializeOwned,
    {
        type Arg = (usize, Option<C>);

        type State = Stage<I, C>;

        fn init(this: ProcessRef<Self>, (stage_ptr, stage_init_state): Self::Arg) -> Self::State {
            let stage_fn = unsafe {
                let stage_handler = stage_ptr as *const ();
                mem::transmute::<*const (), fn(&mut C, Vec<I>) -> ()>(stage_handler)
            };

            Self::State {
                this,
                stage_fn,
                state: Consumer::with_optional_state(stage_init_state),
            }
        }

        fn terminate(state: Self::State) {
            debug!("Shutting down Consumer. State: {:?}", &state.state.state)
        }
    }

    impl<I, C> Consumer<I, C>
    where
        C: Default + Serialize + DeserializeOwned,
        Self: AbstractProcess<Arg = (usize, Option<C>)>,
    {
        fn cast_args(
            stage_fn: fn(&mut C, Vec<I>) -> (),
            state: Option<C>,
        ) -> <Self as AbstractProcess>::Arg {
            let stage_ptr = stage_fn as *const () as usize;
            (stage_ptr, state)
        }

        pub fn start_link_with_handler(
            stage_fn: fn(&mut C, Vec<I>) -> (),
        ) -> ProcessRef<Consumer<I, C>> {
            Consumer::<I, C>::start_link(Self::cast_args(stage_fn, None), None)
        }

        pub fn start_link_with_handler_and_state(
            stage_fn: fn(&mut C, Vec<I>) -> (),
            state: C,
        ) -> ProcessRef<Consumer<I, C>> {
            Consumer::<I, C>::start_link(Self::cast_args(stage_fn, Some(state)), None)
        }
    }

    impl<I, C> MessageHandler<Vec<I>> for Consumer<I, C>
    where
        I: Serialize + DeserializeOwned,
        C: Debug + Default + Serialize + DeserializeOwned,
    {
        fn handle(state: &mut Self::State, events: Vec<I>) {
            (state.stage_fn)(&mut state.inner_state(), events);
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(bound = "")]
    pub struct SubscribeToMessage<I, S, C>(pub ProcessRef<Producer<S, C>>)
    where
        S: ProducerStage<Output = I>,
        C: Default + Serialize + DeserializeOwned;

    impl<I, S, C> RequestHandler<SubscribeToMessage<I, S, C>> for Consumer<I, C>
    where
        I: Serialize + DeserializeOwned,
        S: ProducerStage<Output = I>,
        C: Debug + Default + Serialize + DeserializeOwned,
    {
        type Response = Result<(), ()>;

        fn handle(
            state: &mut Self::State,
            SubscribeToMessage(producer): SubscribeToMessage<I, S, C>,
        ) -> Self::Response {
            producer.request(SubscribeMessage(state.this.clone()))?;

            Ok(())
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(bound = "")]
    pub struct AskDemandMessage<I, S, C>(pub ProcessRef<Producer<S, C>>, pub DemandCount)
    where
        S: ProducerStage<Output = I>,
        C: Debug + Default + Serialize + DeserializeOwned;

    impl<I, S, C> MessageHandler<AskDemandMessage<I, S, C>> for Consumer<I, C>
    where
        I: Serialize + DeserializeOwned,
        S: ProducerStage<Output = I>,
        C: Debug + Default + Serialize + DeserializeOwned,
    {
        fn handle(
            state: &mut Self::State,
            AskDemandMessage(producer, demand): AskDemandMessage<I, S, C>,
        ) {
            let res = producer.request(DemandMessage(state.this.clone(), demand));

            if res.is_ok() {
                state.state.demand = demand;
                producer.send(Ask(demand));
            }
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

pub trait ProducerStage: Debug + Serialize + DeserializeOwned {
    type Output: Serialize + DeserializeOwned + Debug;

    #[must_use]
    fn handle_demand(&mut self, demand: DemandCount) -> Vec<Self::Output>;
}

#[derive(Debug)]
struct Demand<I, C>
where
    C: Default + Serialize + DeserializeOwned,
{
    demand_count: DemandCount,
    process: ProcessRef<Consumer<I, C>>,
}

#[derive(Debug)]
pub struct Producer<S, C>
where
    S: ProducerStage,
    C: Default + Serialize + DeserializeOwned,
{
    demands: LinkedList<Demand<S::Output, C>>,
    max_demand: Option<DemandCount>,
    consumers: HashSet<ProcessRef<Consumer<S::Output, C>>>,
    stage: S,
}

impl<S, C> AbstractProcess for Producer<S, C>
where
    S: ProducerStage,
    C: Default + Serialize + DeserializeOwned,
{
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
#[serde(bound = "")]
pub struct SubscribeMessage<I, C>(pub ProcessRef<Consumer<I, C>>)
where
    C: Default + Serialize + DeserializeOwned;

impl<S, C> RequestHandler<SubscribeMessage<S::Output, C>> for Producer<S, C>
where
    S: ProducerStage,
    C: Default + Serialize + DeserializeOwned,
{
    type Response = Result<(), ()>;
    fn handle(
        state: &mut Self::State,
        SubscribeMessage(consumer): SubscribeMessage<S::Output, C>,
    ) -> Self::Response {
        state.consumers.insert(consumer);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct DemandMessage<I, C>(pub ProcessRef<Consumer<I, C>>, pub DemandCount)
where
    C: Default + Serialize + DeserializeOwned;

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

impl<S, C> RequestHandler<DemandMessage<S::Output, C>> for Producer<S, C>
where
    S: ProducerStage,
    C: Debug + Default + Serialize + DeserializeOwned,
{
    type Response = Result<(), DemandResponse>;

    fn handle(
        state: &mut Self::State,
        DemandMessage(consumer, demand_count): DemandMessage<S::Output, C>,
    ) -> Self::Response {
        //dbg!(demand_count);
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
        queue_demand(state, new_demand);

        Ok(())
    }
}

fn queue_demand<S, C>(
    state: &mut Producer<S, C>,
    new_demand: Demand<<S as ProducerStage>::Output, C>,
) where
    S: ProducerStage,
    C: Debug + Default + Serialize + DeserializeOwned,
{
    if let Some(first) = state.demands.pop_front() {
        if new_demand.demand_count > first.demand_count {
            state.demands.push_front(first);
            state.demands.push_front(new_demand);
        } else {
            state.demands.push_front(new_demand);
            state.demands.push_front(first);
        }
    } else {
        state.demands.push_front(new_demand);
    }

    debug!("{:?}", &state.demands);
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Ask(pub DemandCount);

impl<S, C> MessageHandler<Ask> for Producer<S, C>
where
    S: ProducerStage,
    C: Debug + Default + Serialize + DeserializeOwned,
{
    fn handle(state: &mut Self::State, Ask(demand): Ask) {
        state
            .stage
            .handle_demand(demand)
            .into_iter()
            .chunks(demand)
            .into_iter()
            .for_each(|handle_now| {
                let events = handle_now.collect::<Vec<_>>();
                let demand = events.len();
                debug!("AAA {:?}", demand);
                if let Some(mut consumer_demand) = state.demands.pop_front() {
                    if consumer_demand.demand_count >= demand {
                        consumer_demand.process.send(events);
                        consumer_demand.demand_count -= demand;
                        queue_demand(state, consumer_demand)
                    }
                }
            });
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Ping;

impl<S, C> RequestHandler<Ping> for Producer<S, C>
where
    S: ProducerStage,
    C: Default + Debug + Serialize + DeserializeOwned,
{
    type Response = ();
    fn handle(state: &mut Self::State, _: Ping) {
        //dbg!(&state);
    }
}
