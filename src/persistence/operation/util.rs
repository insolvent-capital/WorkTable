use data_bucket::Link;
use indexset::cdc::change::{self, ChangeEvent};
use indexset::core::pair::Pair;
use std::fmt::Debug;

pub const MAX_CHECK_DEPTH: usize = 30;

pub fn validate_events<T>(
    evs: &mut Vec<ChangeEvent<Pair<T, Link>>>,
) -> Vec<ChangeEvent<Pair<T, Link>>>
where
    T: Debug,
{
    let mut removed_events = vec![];
    let mut finish_condition = false;

    while !finish_condition {
        let (iteration_events, error_pos) = validate_events_iteration(evs);
        if iteration_events.is_empty() {
            finish_condition = true;
        } else {
            let drain_pos = evs.len() - error_pos;
            removed_events.extend(evs.drain(drain_pos..));
        }
    }

    removed_events.sort_by_key(|ev2| std::cmp::Reverse(ev2.id()));

    removed_events
}

fn validate_events_iteration<T>(evs: &[ChangeEvent<Pair<T, Link>>]) -> (Vec<change::Id>, usize) {
    let Some(mut last_ev_id) = evs.last().map(|ev| ev.id()) else {
        return (vec![], 0);
    };
    let mut evs_before_error = vec![last_ev_id];
    let mut rev_evs_iter = evs.iter().rev().skip(1);
    let mut error_flag = false;
    let mut check_depth = 1;

    while !error_flag && check_depth < MAX_CHECK_DEPTH {
        if let Some(next_ev) = rev_evs_iter.next().map(|ev| ev.id()) {
            if last_ev_id.is_next_for(next_ev) || last_ev_id == next_ev {
                check_depth += 1;
                last_ev_id = next_ev;
                evs_before_error.push(last_ev_id);
            } else {
                error_flag = true
            }
        } else {
            break;
        }
    }

    if error_flag {
        (evs_before_error, check_depth)
    } else {
        (vec![], 0)
    }
}
