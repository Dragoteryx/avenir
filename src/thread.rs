use futures::channel::oneshot::*;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread::{JoinHandle, spawn};

#[derive(Debug)]
pub struct Blocking<T> {
	receiver: Receiver<Result<T, Box<dyn Any + Send>>>,
	thread: Option<JoinHandle<()>>,
}

pub fn blocking<F, T>(func: F) -> Blocking<T>
where
	F: FnOnce() -> T + Send + 'static,
	T: Send + 'static,
{
	let (sender, receiver) = channel();
	let thread = Some(spawn(move || {
		let func = AssertUnwindSafe(func);
		let res = catch_unwind(func);
		let _ = sender.send(res);
	}));

	Blocking { receiver, thread }
}

impl<T> Blocking<T> {
	#[inline]
	pub fn detach(mut self) {
		self.thread.take();
	}
}

impl<T> Future for Blocking<T> {
	type Output = T;

	#[track_caller]
	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		match Pin::new(&mut self.receiver).poll(cx) {
			Poll::Ready(Err(_)) => panic!("Thread has panicked"),
			Poll::Ready(Ok(Ok(value))) => Poll::Ready(value),
			Poll::Ready(Ok(Err(err))) => resume_unwind(err),
			Poll::Pending => Poll::Pending,
		}
	}
}

impl<T> Drop for Blocking<T> {
	fn drop(&mut self) {
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}
