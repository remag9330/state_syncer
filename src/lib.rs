use std::{collections::HashMap, marker::PhantomData};

use log::warn;
use thiserror::Error;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct StateVersion(u32);

impl StateVersion {
    fn zero() -> Self {
        Self(0)
    }

    fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    fn states_back(&self, delta: u32) -> Option<Self> {
        if self.0 >= delta {
            Some(Self(self.0 - delta))
        } else {
            None
        }
    }

    fn delta(&self, other: &Self) -> i32 {
        if self.0 >= other.0 {
            (self.0 - other.0) as i32
        } else {
            -((other.0 - self.0) as i32)
        }
    }
}

pub trait State<TEvent, TTickError> {
    fn tick(&mut self, events: &[TEvent]) -> Result<(), TTickError>;
}

pub struct StateSyncer<TState, TEvent, TTickError>
where TState : Clone + State<TEvent, TTickError> {
    current_version: StateVersion,
    earliest_version: StateVersion,
    max_version_latency: i32,
    earliest_dirty_state: Option<StateVersion>,
    
    current_state: TState,
    previous_states: HashMap<StateVersion, TState>,
    most_recent_finalised_state: Option<TState>,

    events: HashMap<StateVersion, Vec<TEvent>>,

    _tick_error_phantom: PhantomData<TTickError>,
}

impl<TState, TEvent, TTickError> StateSyncer<TState, TEvent, TTickError>
where TState: Clone + State<TEvent, TTickError> {
    pub fn new_with_latency(initial_state: TState, max_version_latency: i32) -> Self {
        Self {
            current_version: StateVersion::zero(),
            earliest_version: StateVersion::zero(),
            max_version_latency,
            earliest_dirty_state: None,

            current_state: initial_state,
            previous_states: HashMap::new(),
            most_recent_finalised_state: None,

            events: HashMap::new(),

            _tick_error_phantom: PhantomData,
        }
    }

    pub fn new(initial_state: TState) -> Self {
        Self::new_with_latency(initial_state, 120)
    }

    pub fn tick(&mut self) -> Result<(), TTickError> {
        self.previous_states.insert(self.current_version, self.current_state.clone());

        let events = Self::events_during(&self.events, &self.current_version);
        let result = self.current_state.tick(events);
        
        self.current_version = self.current_version.next();
        self.remove_old_states_and_events();

        result
    }

    fn remove_old_states_and_events(&mut self) {
        let version_to_clear = self.current_version.states_back(self.max_version_latency as u32 + 1);

        if let Some(version_to_clear) = version_to_clear {
            let removed_state = self.previous_states.remove(&version_to_clear);
            self.events.remove(&version_to_clear);

            if let Some(removed_state) = removed_state {
                self.most_recent_finalised_state = Some(removed_state);
            } else {
                warn!("No state was removed while removing old state/events while there was a valid version to clear");
            }

            self.earliest_version = version_to_clear.next();
        }
    }

    fn events_during<'a>(events: &'a HashMap<StateVersion, Vec<TEvent>>, v: &StateVersion) -> &'a [TEvent] {
        events.get(v)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn append_event(&mut self, e: TEvent) -> Result<(), EventInsertionError<TTickError>> {
        self.insert_event_internal(e, self.current_version)?;
        self.reconcile_from_earliest_dirty_state()?;

        Ok(())
    }

    pub fn append_events(&mut self, es: Vec<TEvent>) -> Result<(), EventInsertionError<TTickError>> {
        for e in es {
            self.insert_event_internal(e, self.current_version)?;
        }
        
        self.reconcile_from_earliest_dirty_state()?;

        Ok(())
    }

    pub fn insert_event(&mut self, e: TEvent, v: StateVersion) -> Result<(), EventInsertionError<TTickError>> {
        self.insert_event_internal(e, v)?;
        self.reconcile_from_earliest_dirty_state()?;

        Ok(())
    }

    pub fn insert_events(&mut self, es: Vec<TEvent>, v: StateVersion) -> Result<(), EventInsertionError<TTickError>> {
        for e in es {
            self.insert_event_internal(e, v)?;
        }
        
        self.reconcile_from_earliest_dirty_state()?;

        Ok(())
    }

    pub fn insert_timestamped_events(&mut self, es: Vec<(TEvent, StateVersion)>) -> Result<(), EventInsertionError<TTickError>> {
        for (e, v) in es {
            self.insert_event_internal(e, v)?;
        }

        self.reconcile_from_earliest_dirty_state()?;

        Ok(())
    }

    fn insert_event_internal(&mut self, e: TEvent, v: StateVersion) -> Result<(), EventInsertionError<TTickError>> {
        let ver_delta = self.current_version.delta(&v);
        if ver_delta > self.max_version_latency {
            return Err(EventInsertionError::EventTooOld);
        }

        let events = self.events
            .entry(v)
            .or_insert(vec![]);

        events.push(e);
        self.try_update_earliest_dirty_state(v);

        Ok(())
    }

    fn try_update_earliest_dirty_state(&mut self, v: StateVersion) {
        if v >= self.current_version {
            return;
        }

        if let Some(eds) = self.earliest_dirty_state {
            if v < eds {
                self.earliest_dirty_state = Some(v);
            }
        } else {
            self.earliest_dirty_state = Some(v);
        }
    }

    fn reconcile_from_earliest_dirty_state(&mut self) -> Result<(), EventInsertionError<TTickError>> {
        let eds = match self.earliest_dirty_state {
            Some(eds) => eds,
            None => return Ok(())
        };

        if eds < self.earliest_version {
            return Err(EventInsertionError::EventTooOld)
        }

        self.earliest_dirty_state.take();

        let end = match self.current_version.states_back(1) {
            Some(v) => v,
            None => {
                // Not normal, but we can probably safely ignore
                warn!("Could not go back 1 state in reconcile_from_earliest_dirty_state. This might be an indicator that it was called before any ticking occurred.");
                return Ok(());
            }
        };

        // First, update everything from EDS to most recent previous state (ie: the state before current_state).
        // The states_back(1) above and curr < end below allow the loop to finish on grabbing the second last
        // previous_state, tick it, and update the last previous_state. Updating current_state is done separately below.
        {
            let mut curr = eds;
            while curr < end {
                let events = Self::events_during(&self.events, &curr);
                let state = self.previous_states.get(&curr)
                    .ok_or(EventInsertionError::FailedReconciliationCannotFindPreviousState)?;

                let mut next_state = state.clone();
                next_state.tick(events)?;

                curr = curr.next();

                self.previous_states.insert(curr, next_state);
            }
        }

        let events = Self::events_during(&self.events, &end);
        let state = self.previous_states.get(&end)
            .ok_or(EventInsertionError::FailedReconciliationCannotFindPreviousState)?;

        let mut next_state = state.clone();
        next_state.tick(events)?;

        self.current_state = next_state;

        Ok(())
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum EventInsertionError<TTickError> {
    #[error("Event cannot be inserted into the current state syncer as it's too old to be properly reconciled from")]
    EventTooOld,
    #[error("Event cannot be inserted as it is too old to be reconciled from")]
    FailedReconciliationCannotFindPreviousState,
    #[error("Event cannot be inserted as post-reconciliation failed")]
    FailedReconciliationTick(#[from] TTickError)
}

#[cfg(test)]
mod tests {
    use super::*;


    #[derive(Clone)]
    struct SimpleState {
        value: i32,
        delta: i32,
    }

    fn simple_state(value: i32) -> StateSyncer<SimpleState, SimpleStateEvent, ()> {
        StateSyncer::new(SimpleState { value, delta: -1 })
    }

    fn simple_state_max_latency(value: i32, latency: i32) -> StateSyncer<SimpleState, SimpleStateEvent, ()> {
        StateSyncer::new_with_latency(SimpleState { value, delta: -1 }, latency)
    }

    enum SimpleStateEvent {
        IncrementByTen,
        ChangeDelta(i32),
    }

    impl State<SimpleStateEvent, ()> for SimpleState {
        fn tick(&mut self, events: &[SimpleStateEvent]) -> Result<(), ()> {
            for event in events {
                match event {
                    SimpleStateEvent::IncrementByTen => self.value += 10,
                    &SimpleStateEvent::ChangeDelta(i) => self.delta = i,
                }
            }

            self.value += self.delta;

            Ok(())
        }
    }

    #[test]
    fn simple_tick() {
        let mut state_syncer = simple_state(50);
        state_syncer.tick().unwrap();
        let new_state = state_syncer.current_state;
        assert_eq!(new_state.value, 49);
    }

    #[test]
    fn double_tick() {
        let mut state_syncer = simple_state(50);
        state_syncer.tick().unwrap();
        state_syncer.tick().unwrap();
        let new_state = state_syncer.current_state;
        assert_eq!(new_state.value, 48);
    }

    #[test]
    fn double_tick_with_ongoing_change() {
        let mut state_syncer = simple_state(20);
        let initial_state_version = state_syncer.current_version;

        state_syncer.tick().unwrap();
        assert_eq!(state_syncer.current_state.value, 19);

        state_syncer.tick().unwrap();
        assert_eq!(state_syncer.current_state.value, 18);

        state_syncer.insert_event(SimpleStateEvent::ChangeDelta(5), initial_state_version).unwrap();
        assert_eq!(state_syncer.current_state.value, 30);
    }

    #[test]
    fn simple_event_append() {
        let mut state_syncer = simple_state(50);
        state_syncer.append_event(SimpleStateEvent::IncrementByTen).unwrap();
        state_syncer.tick().unwrap();
        let new_state = state_syncer.current_state;
        assert_eq!(new_state.value, 59);
    }

    #[test]
    fn double_event_append() {
        let mut state_syncer = simple_state(50);
        state_syncer.append_event(SimpleStateEvent::IncrementByTen).unwrap();
        state_syncer.append_event(SimpleStateEvent::IncrementByTen).unwrap();
        state_syncer.tick().unwrap();
        let new_state = state_syncer.current_state;
        assert_eq!(new_state.value, 69);
    }

    #[test]
    fn double_event_append_double_tick_interleaved() {
        let mut state_syncer = simple_state(50);
        state_syncer.append_event(SimpleStateEvent::IncrementByTen).unwrap();
        state_syncer.tick().unwrap();
        state_syncer.append_event(SimpleStateEvent::IncrementByTen).unwrap();
        state_syncer.tick().unwrap();
        let new_state = state_syncer.current_state;
        assert_eq!(new_state.value, 68);
    }

    #[test]
    fn event_inserted() {
        let mut state_syncer = simple_state(50);
        let initial_state_version = state_syncer.current_version;

        state_syncer.tick().unwrap();
        state_syncer.insert_event(SimpleStateEvent::IncrementByTen, initial_state_version).unwrap();
        let new_state = state_syncer.current_state;
        assert_eq!(new_state.value, 59);
    }

    #[test]
    fn max_version_latency_honored() {
        let mut state_syncer = simple_state_max_latency(10, 5); // state 0
        let initial_state_version = state_syncer.current_version;

        state_syncer.tick().unwrap(); // state 1
        assert_eq!(state_syncer.current_state.value, 9);

        state_syncer.tick().unwrap(); // state 2
        assert_eq!(state_syncer.current_state.value, 8);
        
        state_syncer.tick().unwrap(); // state 3
        assert_eq!(state_syncer.current_state.value, 7);
        
        state_syncer.tick().unwrap(); // state 4
        assert_eq!(state_syncer.current_state.value, 6);
        
        state_syncer.insert_event(SimpleStateEvent::IncrementByTen, initial_state_version).unwrap();
        assert_eq!(state_syncer.current_state.value, 16);

        state_syncer.tick().unwrap(); // state 5 (last chance to insert to state 0)
        assert_eq!(state_syncer.current_state.value, 15);

        state_syncer.insert_event(SimpleStateEvent::IncrementByTen, initial_state_version).unwrap();
        assert_eq!(state_syncer.current_state.value, 25);
        
        state_syncer.tick().unwrap(); // state 6 (shouldn't be able to insert back to 0 at this point)
        assert_eq!(state_syncer.current_state.value, 24);

        let r = state_syncer.insert_event(SimpleStateEvent::IncrementByTen, initial_state_version)
            .unwrap_err();

        assert_eq!(r, EventInsertionError::EventTooOld);
    }

    #[test]
    fn ensure_old_states_and_events_are_being_removed() {
        // State 0
        let mut state_syncer = simple_state_max_latency(10, 3);

        assert_eq!(state_syncer.previous_states.len(), 0);
        assert_eq!(state_syncer.events.len(), 0);

        state_syncer.tick().unwrap(); // State 1
        assert_eq!(state_syncer.previous_states.len(), 1);
        assert_eq!(state_syncer.events.len(), 0);

        state_syncer.append_event(SimpleStateEvent::IncrementByTen).unwrap();

        state_syncer.tick().unwrap(); // State 2
        assert_eq!(state_syncer.previous_states.len(), 2);
        assert_eq!(state_syncer.events.len(), 1);
        
        state_syncer.tick().unwrap(); // State 3
        assert_eq!(state_syncer.previous_states.len(), 3);
        assert_eq!(state_syncer.events.len(), 1);
        
        state_syncer.tick().unwrap(); // State 4
        assert_eq!(state_syncer.previous_states.len(), 3);
        assert_eq!(state_syncer.events.len(), 1);
        
        state_syncer.tick().unwrap(); // State 5
        assert_eq!(state_syncer.previous_states.len(), 3);
        assert_eq!(state_syncer.events.len(), 0);
        
        state_syncer.tick().unwrap(); // State 6
        assert_eq!(state_syncer.previous_states.len(), 3);
        assert_eq!(state_syncer.events.len(), 0);
    }

    #[test]
    fn check_most_recent_finalised_state_is_accurate() {
        // State 0
        let mut state_syncer = simple_state_max_latency(20, 3);
        assert!(state_syncer.most_recent_finalised_state.is_none());

        state_syncer.tick().unwrap(); // State 1
        assert!(state_syncer.most_recent_finalised_state.is_none());

        state_syncer.tick().unwrap(); // State 2
        assert!(state_syncer.most_recent_finalised_state.is_none());

        state_syncer.tick().unwrap(); // State 3
        assert!(state_syncer.most_recent_finalised_state.is_none());

        state_syncer.tick().unwrap(); // State 4 (finalised state 0)
        assert_eq!(state_syncer.most_recent_finalised_state.as_ref().unwrap().value, 20);

        state_syncer.tick().unwrap(); // State 5 (finalised state 1)
        assert_eq!(state_syncer.most_recent_finalised_state.as_ref().unwrap().value, 19);
    }
}
