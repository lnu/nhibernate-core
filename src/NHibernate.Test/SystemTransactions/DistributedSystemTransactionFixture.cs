using System;
using System.Linq;
using System.Threading;
using System.Transactions;
using log4net;
using log4net.Repository.Hierarchy;
using NHibernate.Linq;
using NHibernate.Test.TransactionTest;
using NUnit.Framework;

namespace NHibernate.Test.SystemTransactions
{
	[TestFixture]
	public class DistributedSystemTransactionFixture : SystemTransactionFixtureBase
	{
		private static readonly ILog _log = LogManager.GetLogger(typeof(DistributedSystemTransactionFixture));

		protected override bool AppliesTo(Dialect.Dialect dialect)
			=> dialect.SupportsDistributedTransactions && base.AppliesTo(dialect);

		protected override void OnTearDown()
		{
			DodgeTransactionCompletionDelayIfRequired();
			base.OnTearDown();
		}

		[Test]
		public void SupportsEnlistingInDistributed()
		{
			using (new TransactionScope())
			{
				ForceEscalationToDistributedTx.Escalate();

				Assert.AreNotEqual(
					Guid.Empty,
					System.Transactions.Transaction.Current.TransactionInformation.DistributedIdentifier,
					"Transaction lacks a distributed identifier");

				using (var s = OpenSession())
				{
					s.Save(new Person());
					// Ensure the connection is acquired (thus enlisted)
					Assert.DoesNotThrow(s.Flush, "Failure enlisting a connection in a distributed transaction.");
				}
			}
		}

		[Test]
		public void SupportsPromotingToDistributed()
		{
			using (new TransactionScope())
			{
				using (var s = OpenSession())
				{
					s.Save(new Person());
					// Ensure the connection is acquired (thus enlisted)
					s.Flush();
				}
				Assert.DoesNotThrow(() => ForceEscalationToDistributedTx.Escalate(),
					"Failure promoting the transaction to distributed while already having enlisted a connection.");
				Assert.AreNotEqual(
					Guid.Empty,
					System.Transactions.Transaction.Current.TransactionInformation.DistributedIdentifier,
					"Transaction lacks a distributed identifier");
			}
		}

		[Test]
		public void WillNotCrashOnPrepareFailure()
		{
			var tx = new TransactionScope();
			var disposeCalled = false;
			try
			{
				using (var s = OpenSession())
				{
					s.Save(new Person { NotNullData = null }); // Cause a SQL not null constraint violation.
				}

				ForceEscalationToDistributedTx.Escalate();

				tx.Complete();
				disposeCalled = true;
				Assert.Throws<TransactionAbortedException>(tx.Dispose, "Scope disposal has not rollback and throw.");
			}
			finally
			{
				if (!disposeCalled)
				{
					try
					{
						tx.Dispose();
					}
					catch
					{
						// Ignore, if disposed has not been called, another exception has occurred in the try and
						// we should avoid overriding it by the disposal failure.
					}
				}
			}
		}

		[Test]
		public void CanRollbackTransaction([Values(false, true)] bool explicitFlush)
		{
			var tx = new TransactionScope();
			var disposeCalled = false;
			try
			{
				using (var s = OpenSession())
				{
					ForceEscalationToDistributedTx.Escalate(true); //will rollback tx
					s.Save(new Person());

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}
				disposeCalled = true;
				Assert.Throws<TransactionAbortedException>(tx.Dispose, "Scope disposal has not rollback and throw.");
			}
			finally
			{
				if (!disposeCalled)
				{
					try
					{
						tx.Dispose();
					}
					catch
					{
						// Ignore, if disposed has not been called, another exception has occurred in the try and
						// we should avoid overriding it by the disposal failure.
					}
				}
			}

			AssertNoPersons();
		}

		[Test]
		public void CanRollbackTransactionFromScope([Values(false, true)] bool explicitFlush)
		{
			using (new TransactionScope())
			using (var s = OpenSession())
			{
				ForceEscalationToDistributedTx.Escalate();
				s.Save(new Person());

				if (explicitFlush)
					s.Flush();
				// No Complete call for triggering rollback.
			}

			AssertNoPersons();
		}

		[Test]
		[Description("Another action inside the transaction do the rollBack outside nh-session-scope.")]
		public void RollbackOutsideNh([Values(false, true)] bool explicitFlush)
		{
			try
			{
				using (var txscope = new TransactionScope())
				{
					using (var s = OpenSession())
					{
						var person = new Person();
						s.Save(person);

						if (explicitFlush)
							s.Flush();
					}
					ForceEscalationToDistributedTx.Escalate(true); //will rollback tx

					txscope.Complete();
				}

				Assert.Fail("Scope disposal has not rollback and throw.");
			}
			catch (TransactionAbortedException)
			{
				_log.Debug("Transaction aborted.");
			}

			AssertNoPersons();
		}

		[Test]
		[Description("rollback inside nh-session-scope should not commit save and the transaction should be aborted.")]
		public void TransactionInsertWithRollBackFromScope([Values(false, true)] bool explicitFlush)
		{
			using (new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = new Person();
					s.Save(person);
					ForceEscalationToDistributedTx.Escalate();

					if (explicitFlush)
						s.Flush();
				}
				// No Complete call for triggering rollback.
			}
			AssertNoPersons();
		}

		[Test]
		[Description("rollback inside nh-session-scope should not commit save and the transaction should be aborted.")]
		public void TransactionInsertWithRollBackTask([Values(false, true)] bool explicitFlush)
		{
			try
			{
				using (var txscope = new TransactionScope())
				{
					using (var s = OpenSession())
					{
						var person = new Person();
						s.Save(person);
						ForceEscalationToDistributedTx.Escalate(true); //will rollback tx

						if (explicitFlush)
							s.Flush();
					}
					txscope.Complete();
				}

				Assert.Fail("Scope disposal has not rollback and throw.");
			}
			catch (TransactionAbortedException)
			{
				_log.Debug("Transaction aborted.");
			}

			AssertNoPersons();
		}

		[Test]
		[Description(@"Two session in two txscope
 (without an explicit NH transaction)
 and with a rollback in the second dtc and a rollback outside nh-session-scope.")]
		public void TransactionInsertLoadWithRollBackFromScope([Values(false, true)] bool explicitFlush)
		{
			object savedId;
			var createdAt = DateTime.Today;
			using (var txscope = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = new Person { CreatedAt = createdAt };
					savedId = s.Save(person);

					if (explicitFlush)
						s.Flush();
				}
				txscope.Complete();
			}

			using (new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = s.Get<Person>(savedId);
					person.CreatedAt = createdAt.AddMonths(-1);

					if (explicitFlush)
						s.Flush();
				}
				ForceEscalationToDistributedTx.Escalate();

				// No Complete call for triggering rollback.
			}

			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				Assert.AreEqual(createdAt, s.Get<Person>(savedId).CreatedAt, "Entity update was not rollback-ed.");
			}
		}

		[Test]
		[Description(@"Two session in two txscope
 (without an explicit NH transaction)
 and with a rollback in the second dtc and a ForceRollback outside nh-session-scope.")]
		public void TransactionInsertLoadWithRollBackTask([Values(false, true)] bool explicitFlush)
		{
			object savedId;
			var createdAt = DateTime.Today;
			using (var txscope = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = new Person { CreatedAt = createdAt };
					savedId = s.Save(person);

					if (explicitFlush)
						s.Flush();
				}
				txscope.Complete();
			}

			try
			{
				using (var txscope = new TransactionScope())
				{
					using (var s = OpenSession())
					{
						var person = s.Get<Person>(savedId);
						person.CreatedAt = createdAt.AddMonths(-1);

						if (explicitFlush)
							s.Flush();
					}
					ForceEscalationToDistributedTx.Escalate(true);

					_log.Debug("completing the tx scope");
					txscope.Complete();
				}
				_log.Debug("Transaction fail.");
				Assert.Fail("Expected tx abort");
			}
			catch (TransactionAbortedException)
			{
				_log.Debug("Transaction aborted.");
			}

			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				Assert.AreEqual(createdAt, s.Get<Person>(savedId).CreatedAt, "Entity update was not rollback-ed.");
			}
		}

		private int _totalCall;

		[Test, Explicit("Test added for NH-1709 (trying to recreate the issue... without luck). If one thread break the test, you can see the result in the console.")]
		public void MultiThreadedTransaction()
		{
			// Test added for NH-1709 (trying to recreate the issue... without luck)
			// If one thread break the test, you can see the result in the console.
			((Logger)_log.Logger).Level = log4net.Core.Level.Debug;
			var actions = new MultiThreadRunner<object>.ExecuteAction[]
			{
				delegate
					{
						CanRollbackTransaction(false);
						_totalCall++;
					},
				delegate
					{
						RollbackOutsideNh(false);
						_totalCall++;
					},
				delegate
					{
						TransactionInsertWithRollBackTask(false);
						_totalCall++;
					},
				delegate
					{
						TransactionInsertLoadWithRollBackTask(false);
						_totalCall++;
					},
			};
			var mtr = new MultiThreadRunner<object>(20, actions)
			{
				EndTimeout = 5000,
				TimeoutBetweenThreadStart = 5
			};
			mtr.Run(null);
			_log.DebugFormat("{0} calls", _totalCall);
		}

		[Test]
		public void CanDeleteItemInDtc([Values(false, true)] bool explicitFlush)
		{
			object id;
			using (var tx = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					id = s.Save(new Person());

					ForceEscalationToDistributedTx.Escalate();

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}
			}

			DodgeTransactionCompletionDelayIfRequired();

			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				Assert.AreEqual(1, s.Query<Person>().Count(), "Entity not found in database.");
			}

			using (var tx = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					ForceEscalationToDistributedTx.Escalate();

					s.Delete(s.Get<Person>(id));

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}
			}

			DodgeTransactionCompletionDelayIfRequired();

			AssertNoPersons();
		}

		[Test]
		[Description("Open/Close a session inside a TransactionScope fails.")]
		public void NH1744()
		{
			using (new TransactionScope())
			{
				using (var s = OpenSession())
				{
					s.Flush();
				}

				using (var s = OpenSession())
				{
					s.Flush();
				}

				//and I always leave the transaction disposed without calling tx.Complete(), I let the database server to rollback all actions in this test.
			}
		}

		[Test]
		public void CanUseSessionWithManyScopes([Values(false, true)] bool explicitFlush)
		{
			// Note that this fails with ConnectionReleaseMode.OnClose and SqlServer:
			// System.Data.SqlClient.SqlException : Microsoft Distributed Transaction Coordinator (MS DTC) has stopped this transaction.
			// Not much an issue since it is advised to not use ConnectionReleaseMode.OnClose.
			using (var s = OpenSession())
			//using (var s = Sfi.WithOptions().ConnectionReleaseMode(ConnectionReleaseMode.OnClose).OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					ForceEscalationToDistributedTx.Escalate();
					// Acquire the connection
					var count = s.Query<Person>().Count();
					Assert.That(count, Is.EqualTo(0), "Unexpected initial entity count.");
					tx.Complete();
				}
				// No dodge here please! Allow to check chaining usages do not fail.
				using (var tx = new TransactionScope())
				{
					s.Save(new Person());

					ForceEscalationToDistributedTx.Escalate();

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}

				DodgeTransactionCompletionDelayIfRequired();

				using (var tx = new TransactionScope())
				{
					ForceEscalationToDistributedTx.Escalate();
					var count = s.Query<Person>().Count();
					Assert.That(count, Is.EqualTo(1), "Unexpected entity count after committed insert.");
					tx.Complete();
				}
				using (new TransactionScope())
				{
					s.Save(new Person());

					ForceEscalationToDistributedTx.Escalate();

					if (explicitFlush)
						s.Flush();

					// No complete for rollback-ing.
				}

				DodgeTransactionCompletionDelayIfRequired();

				// Do not reuse the session after a rollback, its state does not allow it.
				// http://nhibernate.info/doc/nhibernate-reference/manipulatingdata.html#manipulatingdata-endingsession-commit
			}

			using (var s = OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					ForceEscalationToDistributedTx.Escalate();
					var count = s.Query<Person>().Count();
					Assert.That(count, Is.EqualTo(1), "Unexpected entity count after rollback-ed insert.");
					tx.Complete();
				}
			}
		}

		[Test]
		public void CanUseSessionOutsideOfScopeAfterScope([Values(false, true)] bool explicitFlush)
		{
			// Note that this fails with ConnectionReleaseMode.OnClose and Npgsql (< 3.2.5?):
			// NpgsqlOperationInProgressException: The connection is already in state 'Executing'
			// Not much an issue since it is advised to not use ConnectionReleaseMode.OnClose.
			using (var s = OpenSession())
			//using (var s = Sfi.WithOptions().ConnectionReleaseMode(ConnectionReleaseMode.OnClose).OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					s.Save(new Person());

					ForceEscalationToDistributedTx.Escalate();

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}
				var count = 0;
				Assert.DoesNotThrow(() => count = s.Query<Person>().Count(), "Failed using the session after scope.");
				if (count != 1)
					// We are not testing that here, so just issue a warning. Do not use DodgeTransactionCompletionDelayIfRequired
					// before previous assert. We want to ascertain the session is usable in any cases.
					Assert.Warn("Unexpected entity count: {0} instead of {1}. The transaction seems to have a delayed commit.", count, 1);
			}
		}

		[Test(Description = "Do not fail, but warn in case a delayed after scope disposal commit is made.")]
		public void DelayedTransactionCompletion([Values(false, true)] bool explicitFlush)
		{
			for (var i = 1; i <= 10; i++)
			{
				// Isolation level must be read committed on the control session: reading twice while expecting some data insert
				// in between due to a late commit. Repeatable read would block and read uncommitted would see the uncommitted data.
				using (var controlSession = OpenSession())
				using (controlSession.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
				{
					// We want to have the control session as ready to query as possible, thus beginning its
					// transaction early for acquiring the connection, even if we will not use it before 
					// below scope completion.

					using (var tx = new TransactionScope())
					{
						using (var s = OpenSession())
						{
							s.Save(new Person());

							ForceEscalationToDistributedTx.Escalate();

							if (explicitFlush)
								s.Flush();
						}
						tx.Complete();
					}

					var count = controlSession.Query<Person>().Count();
					if (count != i)
					{
						// See https://github.com/npgsql/npgsql/issues/1571#issuecomment-308651461 discussion with a Microsoft
						// employee: MSDTC consider a transaction to be committed once it has collected all participant votes
						// for committing from prepare phase. It then immediately notifies all participants of the outcome.
						// This causes TransactionScope.Dispose to leave while the second phase of participants may still
						// be executing. This means the transaction from the db view point can still be pending and not yet
						// committed. This is by design of MSDTC and we have to cope with that. Some data provider may have
						// a global locking mechanism causing any subsequent use to wait for the end of the commit phase,
						// but this is not the usual case. Some other, as Npgsql < v3.2.5, may crash due to this, because
						// they re-use the connection for the second phase.
						Thread.Sleep(100);
						var countSecondTry = controlSession.Query<Person>().Count();
						Assert.Warn($"Unexpected entity count: {count} instead of {i}. " +
							"This may mean current data provider has a delayed commit, occurring after scope disposal. " +
							$"After waiting, count is now {countSecondTry}. ");
						break;
					}
				}
			}
		}

		private void DodgeTransactionCompletionDelayIfRequired()
		{
			if (Sfi.ConnectionProvider.Driver.HasDelayedDistributedTransactionCompletion)
				Thread.Sleep(500);
		}

		public class ForceEscalationToDistributedTx : IEnlistmentNotification
		{
			private readonly bool _shouldRollBack;
			private readonly int _thread;

			public static void Escalate(bool shouldRollBack = false)
			{
				var force = new ForceEscalationToDistributedTx(shouldRollBack);
				System.Transactions.Transaction.Current.EnlistDurable(Guid.NewGuid(), force, EnlistmentOptions.None);
			}

			private ForceEscalationToDistributedTx(bool shouldRollBack)
			{
				_shouldRollBack = shouldRollBack;
				_thread = Thread.CurrentThread.ManagedThreadId;
			}

			public void Prepare(PreparingEnlistment preparingEnlistment)
			{
				Assert.AreNotEqual(_thread, Thread.CurrentThread.ManagedThreadId);
				if (_shouldRollBack)
				{
					_log.Debug(">>>>Force Rollback<<<<<");
					preparingEnlistment.ForceRollback();
				}
				else
				{
					preparingEnlistment.Prepared();
				}
			}

			public void Commit(Enlistment enlistment)
			{
				enlistment.Done();
			}

			public void Rollback(Enlistment enlistment)
			{
				enlistment.Done();
			}

			public void InDoubt(Enlistment enlistment)
			{
				enlistment.Done();
			}
		}
	}
}