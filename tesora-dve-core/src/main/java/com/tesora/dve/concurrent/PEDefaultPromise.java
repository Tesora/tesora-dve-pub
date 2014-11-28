package com.tesora.dve.concurrent;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

public class PEDefaultPromise<T> implements CompletionTarget<T>, CompletionHandle<T>, CompletionNotifier<T> {
	static final Object NO_RESULT = new Object();
	
	static Logger logger = Logger.getLogger( PEDefaultPromise.class );

	static AtomicInteger nextId = new AtomicInteger();
	int thisId = nextId.incrementAndGet();

	AtomicReference<PromiseState> state = new AtomicReference<>(new PromiseState());

	@SuppressWarnings("unchecked")
	@Override
	public void addListener(CompletionTarget<T> listener) {
		do {
			PromiseState<T> current = state.get();
			if (current.isFinished()) {

				if (current.isSuccess())
					listener.success((T)current.result);
				else
					listener.failure(current.error);

				break;
			} else {
				PromiseState next = new PromiseState(listener,current);
				if (state.compareAndSet(current,next))
					break;
			}
		} while (true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void success(T returnValue) {
		do {
			PromiseState<T> current = state.get();
			if (current.isFinished())
				break;

			PromiseState<T> next = new PromiseState<>(returnValue,current);
			if (state.compareAndSet(current, next)) {
				notifyListeners(next);
				break;
			}
		} while (true);
	}

	private void notifyListeners(PromiseState<T> finishState) {
		if (finishState.isSuccess())
			notifySuccess(finishState);
		else
			notifyFailure(finishState);
	}

	@SuppressWarnings("unchecked")
	private void notifySuccess(PromiseState<T> finishState) {
		T result = (T)finishState.result;
		while (finishState != null){
			if (finishState.listener != null)
				finishState.listener.success(result);
			finishState = finishState.next;
		}
	}

	private void notifyFailure(PromiseState<T> finishState) {
		Exception error = finishState.error;
		while (finishState != null){
			if (finishState.listener != null)
				finishState.listener.failure(error);
			finishState = finishState.next;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean trySuccess(T returnValue) {
		do {
			PromiseState<T> current = state.get();
			if (current.isFinished())
				return false;

			PromiseState<T> next = new PromiseState<>(returnValue,current);
			if (state.compareAndSet(current, next)) {
				notifyListeners(next);
				break;
			}
		} while (true);
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void failure(Exception t) {
		do {
			PromiseState<T> current = state.get();
			if (current.isFinished())
				break;

			PromiseState<T> next = new PromiseState<>(t,current);
			if (state.compareAndSet(current, next)) {
				notifyListeners(next);
				break;
			}
		} while (true);
	}

	@Override
	public boolean isFulfilled() {
		return state.get().isFinished();
	}

	@Override
	public String toString() {
		return "PEDefaultPromise{"+thisId+"}";
	}



	static class PromiseState<T> {
		final Object result;
		final Exception error;
		final CompletionTarget<T> listener;
		final PromiseState<T> next;

		public PromiseState() {
			this.result = NO_RESULT;
			this.error = null;
			this.listener = null;
			this.next = null;
		}

		public PromiseState(CompletionTarget<T> listener, PromiseState<T> current) {
			this.result = NO_RESULT;
			this.error = null;
			this.listener = listener;
			this.next = current;
		}

		public PromiseState(T result, PromiseState<T> current) {
			this.result = result;
			this.error = null;
			this.listener = null;
			this.next = current;
		}

		public PromiseState(Exception error, PromiseState<T> current) {
			this.result = NO_RESULT;
			this.error = error;
			this.listener = null;
			this.next = current;
		}

		public boolean isFinished(){
			return isError() || isSuccess();
		}

		public boolean isError(){
			return error != null;
		}

		public boolean isSuccess(){
			return result != NO_RESULT;
		}

	}

}
