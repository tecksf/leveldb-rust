use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::AtomicBool;
use std::thread;

pub type Task = Box<dyn FnOnce() + Send>;

pub struct Dispatcher {
    tasks: Arc<Mutex<VecDeque<Task>>>,
    background_thread: Option<thread::JoinHandle<()>>,
    background_thread_cv: Arc<Condvar>,
    alive: Arc<AtomicBool>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(VecDeque::new())),
            background_thread_cv: Arc::new(Condvar::new()),
            background_thread: None,
            alive: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn dispatch(&mut self, task: Task) {
        let mut tasks = self.tasks.lock().unwrap();

        if self.background_thread.is_none() {
            self.alive.store(true, std::sync::atomic::Ordering::Relaxed);
            let tasks = self.tasks.clone();
            let background_thread_cv = self.background_thread_cv.clone();
            let alive = self.alive.clone();
            self.background_thread = Some(thread::spawn(
                move || Self::background_entry(tasks, background_thread_cv, alive)
            ));
        }

        if tasks.is_empty() {
            self.background_thread_cv.notify_one();
        }

        tasks.push_back(task);
    }

    pub fn terminate(&mut self) {
        self.alive.store(false, std::sync::atomic::Ordering::Relaxed);
        self.background_thread_cv.notify_one();
        if let Some(handle) = self.background_thread.take() {
            handle.join().unwrap();
        }
    }

    fn background_entry(tasks: Arc<Mutex<VecDeque<Task>>>, background_thread_cv: Arc<Condvar>, alive: Arc<AtomicBool>) {
        while alive.load(std::sync::atomic::Ordering::Relaxed) {
            let mut tasks = tasks.lock().unwrap();

            while tasks.is_empty() {
                tasks = background_thread_cv.wait(tasks).unwrap();
                if !alive.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
            }

            let task = tasks.pop_front().unwrap();
            drop(tasks);
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
        assert!(dispatcher.background_thread.is_none());
        assert!(!dispatcher.alive.load(std::sync::atomic::Ordering::Relaxed));
    }
}
