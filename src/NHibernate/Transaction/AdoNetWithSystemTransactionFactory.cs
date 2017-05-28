using System;
using System.Collections;
using System.Linq;
using System.Threading;
using System.Transactions;
using NHibernate.Engine;
using NHibernate.Engine.Transaction;
using NHibernate.Impl;

namespace NHibernate.Transaction
{
	public class AdoNetWithSystemTransactionFactory : ITransactionFactory
	{
		private static readonly IInternalLogger _logger = LoggerProvider.LoggerFor(typeof(ITransactionFactory));

		private readonly AdoNetTransactionFactory _adoNetTransactionFactory = new AdoNetTransactionFactory();

		public void Configure(IDictionary props)
		{
		}

		public ITransaction CreateTransaction(ISessionImplementor session)
		{
			return new AdoTransaction(session);
		}

		public void EnlistInSystemTransactionIfNeeded(ISessionImplementor session)
		{
			// Handle the transaction on the originating session only.
			var originatingSession = session.ConnectionManager.Session;

			if (!session.ConnectionManager.ShouldAutoJoinTransaction)
			{
				return;
			}

			// Ensure the session does not run on a thread supposed to be blocked, waiting
			// for transaction completion.
			originatingSession.TransactionContext?.WaitOne();

			var transaction = System.Transactions.Transaction.Current;
			// We may have defined the transaction context before having the connection, so we
			// need to ensure enlistment even when the transaction context is already defined.
			// But avoid redefining to a sub-scope transaction.
			if (originatingSession.TransactionContext == null ||
				((SystemTransactionContext)originatingSession.TransactionContext).AmbientTransaction == transaction)
			{
				originatingSession.ConnectionManager.EnlistIfRequired(transaction);
			}

			if (transaction == null)
				return;

			if (originatingSession.TransactionContext != null)
			{
				if (session.TransactionContext == null)
					// New dependent session
					session.TransactionContext = new DependentContext(originatingSession.TransactionContext);
				return;
			}

			var transactionContext = new SystemTransactionContext(originatingSession, transaction);
			transactionContext.AmbientTransaction.EnlistVolatile(
				transactionContext,
				EnlistmentOptions.EnlistDuringPrepareRequired);
			originatingSession.TransactionContext = transactionContext;

			_logger.DebugFormat(
				"enlisted into DTC transaction: {0}",
				transactionContext.AmbientTransaction.IsolationLevel);

			originatingSession.AfterTransactionBegin(null);
			foreach (var dependentSession in originatingSession.ConnectionManager.DependentSessions)
			{
				dependentSession.TransactionContext = new DependentContext(transactionContext);
				dependentSession.AfterTransactionBegin(null);
			}
		}

		public bool IsInActiveSystemTransaction(ISessionImplementor session)
			=> session.TransactionContext?.IsInActiveTransaction ?? false;

		public void ExecuteWorkInIsolation(ISessionImplementor session, IIsolatedWork work, bool transacted)
		{
			using (var tx = new TransactionScope(TransactionScopeOption.Suppress))
			{
				// instead of duplicating the logic, we suppress the system transaction and create
				// our own transaction instead
				_adoNetTransactionFactory.ExecuteWorkInIsolation(session, work, transacted);
				tx.Complete();
			}
		}

		public class SystemTransactionContext : ITransactionContext, IEnlistmentNotification
		{
			internal System.Transactions.Transaction AmbientTransaction { get; private set; }
			public bool ShouldCloseSessionOnSystemTransactionCompleted { get; set; }
			public bool IsInActiveTransaction { get; internal set; }

			private readonly ISessionImplementor _sessionImplementor;
			private volatile SemaphoreSlim _semaphore;
			private volatile bool _locked;
			private readonly AsyncLocal<bool> _bypassWait = new AsyncLocal<bool>();
			private bool IsDistributed => AmbientTransaction.TransactionInformation.DistributedIdentifier != Guid.Empty;

			public SystemTransactionContext(
				ISessionImplementor sessionImplementor,
				System.Transactions.Transaction transaction)
			{
				_sessionImplementor = sessionImplementor;
				AmbientTransaction = transaction.Clone();
				AmbientTransaction.TransactionCompleted += TransactionCompleted;
				IsInActiveTransaction = true;
			}

			public void WaitOne()
			{
				if (_isDisposed)
					return;
				if (!_locked && AmbientTransaction.TransactionInformation.Status != TransactionStatus.Active)
					// Rollback case may end the transaction without a prepare phase, apply the lock.
					Lock();
				// Volatile member, do not simplify by removing the local variable.
				var semaphore = _semaphore;
				if (semaphore == null || _bypassWait.Value)
					return;
				if (_bypassWait.Value)
					return;
				try
				{
					if (semaphore.Wait(5000))
						return;
					// A call occurring after transaction scope disposal should not have to wait long, since
					// the scope disposal is supposed to block until the transaction has completed: I hope
					// that it at least ensures IO are done, even if experience shows DTC lets the scope
					// disposal leave before having finished with volatile ressources and
					// TransactionCompleted event.
					// Remove the block then throw.
					Unlock();
					throw new HibernateException(
						"Synchronization timeout for transaction completion. This may be a bug in NHibernate.");
				}
				catch (HibernateException)
				{
					throw;
				}
				catch (Exception ex)
				{
					_logger.Warn(
						"Synchronization failure, assuming it has been concurrently disposed and does not need sync anymore.",
						ex);
				}
			}

			private void Lock()
			{
				if (_locked || _isDisposed)
					return;
				_locked = true;
				_semaphore = new SemaphoreSlim(0);
			}

			private void Unlock()
			{
				// Do not set _locked back to false, we want to wait only once per transaction context.
				// Volatile member, do not simplify by removing the local variable.
				var semaphore = _semaphore;
				_semaphore = null;
				if (semaphore == null)
					return;
				// Not supposed to have more than one waiting thread, but in case the user made bad multi-threading,
				// do not leave frozen threads.
				semaphore.Release(100);
				semaphore.Dispose();
			}

			#region IEnlistmentNotification Members

			void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment)
			{
				using (new SessionIdLoggingContext(_sessionImplementor.SessionId))
				{
					try
					{
						using (var tx = new TransactionScope(AmbientTransaction))
						{
							if (_sessionImplementor.ConnectionManager.IsConnected)
							{
								using (_sessionImplementor.ConnectionManager.BeginsFlushingFromSystemTransaction(IsDistributed))
								{
									_sessionImplementor.BeforeTransactionCompletion(null);
									foreach (var dependentSession in _sessionImplementor.ConnectionManager.DependentSessions)
										dependentSession.BeforeTransactionCompletion(null);

									_logger.Debug("prepared for system transaction");

									tx.Complete();
								}
							}
						}
						// Lock the session to ensure second phase gets done before the session is used by code following
						// the transaction scope disposal.
						Lock();

						preparingEnlistment.Prepared();
					}
					catch (Exception exception)
					{
						_logger.Error("System transaction prepare phase failed", exception);
						preparingEnlistment.ForceRollback(exception);
					}
				}
			}

			void IEnlistmentNotification.Commit(Enlistment enlistment)
				=> ProcessSecondPhase(enlistment, true);

			void IEnlistmentNotification.Rollback(Enlistment enlistment)
				=> ProcessSecondPhase(enlistment, false);

			void IEnlistmentNotification.InDoubt(Enlistment enlistment)
				=> ProcessSecondPhase(enlistment, null);

			private void ProcessSecondPhase(Enlistment enlistment, bool? success)
			{
				using (new SessionIdLoggingContext(_sessionImplementor.SessionId))
				{
					_logger.Debug(
						success.HasValue
							? success.Value
								? "Committing system transaction"
								: "Rolled back system transaction"
							: "System transaction is in doubt");
					// we have not much to do here, since it is the actual
					// DB connection that will commit/rollback the transaction
					// Usual cases will raise after transaction actions from TransactionCompleted event.
					if (!success.HasValue)
					{
						// In-doubt. A durable ressource has failed and may recover, but we won't wait to know.
						RunAfterTransactionActions(false);
					}

					enlistment.Done();
				}
			}

			#endregion

			private void TransactionCompleted(object sender, TransactionEventArgs e)
			{
				e.Transaction.TransactionCompleted -= TransactionCompleted;
				// This event may execute before second phase, so we cannot try to get the success from second phase.
				// Using this event is required by example in case the prepare phase failed and called force rollback:
				// no second phase would occur for this ressource. Maybe this may happen in some other circumstances
				// too.
				var wasSuccessful = false;
				try
				{
					wasSuccessful =
						AmbientTransaction.TransactionInformation.Status == TransactionStatus.Committed;
				}
				catch (ObjectDisposedException ode)
				{
					_logger.Warn("Completed transaction was disposed, assuming transaction rollback", ode);
				}
				RunAfterTransactionActions(wasSuccessful);
			}

			private volatile bool _afterTransactionActionsDone;

			private void RunAfterTransactionActions(bool wasSuccessful)
			{
				if (_afterTransactionActionsDone)
					// Probably called from In-Doubt and TransactionCompleted.
					return;
				_afterTransactionActionsDone = true;
				// Allow transaction completed actions to run while others stay blocked.
				_bypassWait.Value = true;
				try
				{
					using (new SessionIdLoggingContext(_sessionImplementor.SessionId))
					{
						// Flag active as false before running actions, otherwise the connection manager will refuse
						// releasing the connection.
						IsInActiveTransaction = false;
						_sessionImplementor.ConnectionManager.AfterTransaction();
						_sessionImplementor.AfterTransactionCompletion(wasSuccessful, null);
						foreach (var dependentSession in _sessionImplementor.ConnectionManager.DependentSessions)
							dependentSession.AfterTransactionCompletion(wasSuccessful, null);

						Cleanup(_sessionImplementor);
					}
				}
				finally
				{
					// Dispose releases blocked threads by the way.
					Dispose();
				}
			}

			private static void Cleanup(ISessionImplementor session)
			{
				foreach (var dependentSession in session.ConnectionManager.DependentSessions.ToList())
				{
					var dependentContext = dependentSession.TransactionContext;
					// Avoid a race condition with session disposal. (Protected on session side by WaitOne,
					// but better have more safety.)
					dependentSession.TransactionContext = null;
					if (dependentContext == null)
						continue;
					if (dependentContext.ShouldCloseSessionOnSystemTransactionCompleted)
						// This changes the enumerated collection.
						dependentSession.CloseSessionFromSystemTransaction();
					dependentContext.Dispose();
				}
				var context = session.TransactionContext;
				// Avoid a race condition with session disposal. (Protected on session side by WaitOne,
				// but better have more safety.)
				session.TransactionContext = null;
				if (context.ShouldCloseSessionOnSystemTransactionCompleted)
				{
					session.CloseSessionFromSystemTransaction();
				}
				// No dispose, done later.
			}

			private volatile bool _isDisposed;

			public void Dispose()
			{
				if (_isDisposed)
					// Avoid disposing twice.
					return;
				_isDisposed = true;
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			protected virtual void Dispose(bool disposing)
			{
				if (disposing)
				{
					AmbientTransaction?.Dispose();
					AmbientTransaction = null;
					Unlock();
				}
			}
		}

		public class DependentContext : ITransactionContext
		{
			public bool IsInActiveTransaction
				=> _mainTransactionContext.IsInActiveTransaction;

			public bool ShouldCloseSessionOnSystemTransactionCompleted { get; set; }

			private readonly ITransactionContext _mainTransactionContext;

			public DependentContext(ITransactionContext mainTransactionContext)
			{
				_mainTransactionContext = mainTransactionContext;
			}

			public void WaitOne() =>
				_mainTransactionContext.WaitOne();

			public void Dispose() { }
		}
	}
}