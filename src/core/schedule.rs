use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub type Task = Box<dyn FnOnce() + Send>;

struct Cron {
    tasks: VecDeque<Task>,
    background_thread: Option<thread::JoinHandle<()>>,
    alive: bool,
}

#[derive(Clone)]
pub struct Dispatcher {
    cron: Arc<Mutex<Cron>>,
    background_thread_cv: Arc<Condvar>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            cron: Arc::new(Mutex::new(Cron {
                tasks: VecDeque::new(),
                background_thread: None,
                alive: true,
            })),
            background_thread_cv: Arc::new(Condvar::new()),
        }
    }

    pub fn dispatch(&self, task: Task) {
        let mut cron = self.cron.lock().unwrap();

        if !cron.alive {
            return;
        }

        if cron.background_thread.is_none() {
            let cron_clone = self.cron.clone();
            let cv = self.background_thread_cv.clone();
            cron.background_thread = Some(thread::spawn(
                move || Self::background_entry(cron_clone, cv)
            ));
        }

        if cron.tasks.is_empty() {
            self.background_thread_cv.notify_one();
        }

        cron.tasks.push_back(task);
    }

    pub fn terminate(&self) {
        let background_thread;
        {
            let mut cron = self.cron.lock().unwrap();
            cron.alive = false;
            background_thread = cron.background_thread.take();
        }
        self.background_thread_cv.notify_one();
        if let Some(handle) = background_thread {
            log::info!("Waiting background work finish...");
            handle.join().unwrap();
        }
    }

    fn background_entry(cron_: Arc<Mutex<Cron>>, background_thread_cv: Arc<Condvar>) {
        loop {
            let mut cron = cron_.lock().unwrap();
            while cron.alive && cron.tasks.is_empty() {
                cron = background_thread_cv.wait(cron).unwrap();
            }

            if !cron.alive {
                break;
            }

            let task = cron.tasks.pop_front().unwrap();
            drop(cron);
            task();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use super::Dispatcher;

    #[test]
    fn test_task_schedule() {
        let result = Arc::new(Mutex::new(0));
        let mut dispatcher = Dispatcher::new();

        for i in 0..10 {
            let number = result.clone();
            dispatcher.dispatch(Box::new(move || {
                let mut num = number.lock().unwrap();
                *num += 10;
            }));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        dispatcher.terminate();
        assert_eq!(*result.lock().unwrap(), 100);
        assert!(dispatcher.cron.lock().unwrap().background_thread.is_none());
        assert!(!dispatcher.cron.lock().unwrap().alive);
    }
}
