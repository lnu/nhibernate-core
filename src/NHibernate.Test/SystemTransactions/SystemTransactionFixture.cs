using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Transactions;
using NHibernate.Linq;
using NHibernate.Test.TransactionTest;
using NUnit.Framework;

namespace NHibernate.Test.SystemTransactions
{
	[TestFixture]
	public class SystemTransactionFixture : SystemTransactionFixtureBase
	{
		protected override bool UseConnectionOnSystemTransactionPrepare => true;

		[Test]
		public void WillNotCrashOnPrepareFailure()
		{
			IgnoreIfUnsupported(false);
			var tx = new TransactionScope();
			var disposeCalled = false;
			try
			{
				using (var s = OpenSession())
				{
					s.Save(new Person { NotNullData = null }); // Cause a SQL not null constraint violation.
				}

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

		[Theory]
		public void CanRollbackTransactionFromScope(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
			using (new TransactionScope())
			using (var s = OpenSession())
			{
				s.Save(new Person());

				if (explicitFlush)
					s.Flush();
				// No Complete call for triggering rollback.
			}

			AssertNoPersons();
		}

		[Theory]
		[Description("rollback inside nh-session-scope should not commit save and the transaction should be aborted.")]
		public void TransactionInsertWithRollBackFromScope(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
			using (new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = new Person();
					s.Save(person);

					if (explicitFlush)
						s.Flush();
				}
				// No Complete call for triggering rollback.
			}
			AssertNoPersons();
		}

		[Theory]
		[Description(@"Two session in two txscope
 (without an explicit NH transaction)
 and with a rollback in the second and a rollback outside nh-session-scope.")]
		public void TransactionInsertLoadWithRollBackFromScope(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
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

				// No Complete call for triggering rollback.
			}

			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				Assert.AreEqual(createdAt, s.Get<Person>(savedId).CreatedAt, "Entity update was not rollback-ed.");
			}
		}

		[Theory]
		public void CanDeleteItem(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
			object id;
			using (var tx = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					id = s.Save(new Person());

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}
			}

			using (var s = OpenSession())
			using (s.BeginTransaction())
			{
				Assert.AreEqual(1, s.Query<Person>().Count(), "Entity not found in database.");
			}

			using (var tx = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					s.Delete(s.Get<Person>(id));

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}
			}

			AssertNoPersons();
		}

		[Theory]
		public void CanUseSessionWithManyScopes(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
			using (var s = Sfi.WithOptions().ConnectionReleaseMode(ConnectionReleaseMode.OnClose).OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					// Acquire the connection
					var count = s.Query<Person>().Count();
					Assert.That(count, Is.EqualTo(0), "Unexpected initial entity count.");
					tx.Complete();
				}

				using (var tx = new TransactionScope())
				{
					s.Save(new Person());

					if (explicitFlush)
						s.Flush();

					tx.Complete();
				}

				using (var tx = new TransactionScope())
				{
					var count = s.Query<Person>().Count();
					Assert.That(count, Is.EqualTo(1), "Unexpected entity count after committed insert.");
					tx.Complete();
				}

				using (new TransactionScope())
				{
					s.Save(new Person());

					if (explicitFlush)
						s.Flush();

					// No complete for rollback-ing.
				}

				// Do not reuse the session after a rollback, its state does not allow it.
				// http://nhibernate.info/doc/nhibernate-reference/manipulatingdata.html#manipulatingdata-endingsession-commit
			}

			using (var s = OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					var count = s.Query<Person>().Count();
					Assert.That(count, Is.EqualTo(1), "Unexpected entity count after rollback-ed insert.");
					tx.Complete();
				}
			}
		}

		[Theory]
		public void CanUseSessionOutsideOfScopeAfterScope(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
			using (var s = Sfi.WithOptions().ConnectionReleaseMode(ConnectionReleaseMode.OnClose).OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					s.Save(new Person());

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

		[Theory]
		[Description("Do not fail, but warn in case a delayed after scope disposal commit is made.")]
		public void DelayedTransactionCompletion(bool explicitFlush)
		{
			IgnoreIfUnsupported(explicitFlush);
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

							if (explicitFlush)
								s.Flush();
						}
						tx.Complete();
					}

					var count = controlSession.Query<Person>().Count();
					if (count != i)
					{
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

		[Test]
		public void FlushFromTransactionAppliesToDisposedSharingSession()
		{
			IgnoreIfUnsupported(false);

			var flushOrder = new List<int>();
			using (var s = OpenSession(new TestInterceptor(0, flushOrder)))
			{
				var builder = s.SessionWithOptions().Connection();

				using (var t = new TransactionScope())
				{
					var p1 = new Person();
					var p2 = new Person();
					var p3 = new Person();
					var p4 = new Person();

					using (var s1 = builder.Interceptor(new TestInterceptor(1, flushOrder)).OpenSession())
						s1.Save(p1);
					using (var s2 = builder.Interceptor(new TestInterceptor(2, flushOrder)).OpenSession())
					{
						s2.Save(p2);
						using (var s3 = s2.SessionWithOptions().Connection().Interceptor(new TestInterceptor(3, flushOrder)).OpenSession())
							s3.Save(p3);
					}
					s.Save(p4);
					t.Complete();
				}
			}

			Assert.That(flushOrder, Is.EqualTo(new[] { 0, 1, 2, 3 }));

			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				Assert.That(s.Query<Person>().Count(), Is.EqualTo(4));
				t.Commit();
			}
		}

		[Test]
		public void FlushFromTransactionAppliesToSharingSession()
		{
			IgnoreIfUnsupported(false);

			var flushOrder = new List<int>();
			using (var s = OpenSession(new TestInterceptor(0, flushOrder)))
			{
				var builder = s.SessionWithOptions().Connection();

				using (var s1 = builder.Interceptor(new TestInterceptor(1, flushOrder)).OpenSession())
				using (var s2 = builder.Interceptor(new TestInterceptor(2, flushOrder)).OpenSession())
				using (var s3 = s2.SessionWithOptions().Connection().Interceptor(new TestInterceptor(3, flushOrder)).OpenSession())
				using (var t = new TransactionScope())
				{
					var p1 = new Person();
					var p2 = new Person();
					var p3 = new Person();
					var p4 = new Person();
					s1.Save(p1);
					s2.Save(p2);
					s3.Save(p3);
					s.Save(p4);
					t.Complete();
				}
			}

			Assert.That(flushOrder, Is.EqualTo(new[] { 0, 1, 2, 3 }));

			using (var s = OpenSession())
			using (var t = s.BeginTransaction())
			{
				Assert.That(s.Query<Person>().Count(), Is.EqualTo(4));
				t.Commit();
			}
		}

		// Taken and adjusted from NH1632 When_commiting_items_in_DTC_transaction_will_add_items_to_2nd_level_cache
		[Test]
		public void WhenCommittingItemsAfterSessionDisposalWillAddThemTo2ndLevelCache()
		{
			int id;
			const string notNullData = "test";
			using (var tx = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = new CacheablePerson { NotNullData = notNullData };
					s.Save(person);
					id = person.Id;

					s.Flush();
				}
				tx.Complete();
			}

			using (var tx = new TransactionScope())
			{
				using (var s = OpenSession())
				{
					var person = s.Load<CacheablePerson>(id);
					Assert.That(person.NotNullData, Is.EqualTo(notNullData));
				}
				tx.Complete();
			}

			// Closing the connection to ensure we can't actually use it.
			var connection = Sfi.ConnectionProvider.GetConnection();
			Sfi.ConnectionProvider.CloseConnection(connection);

			// The session is supposed to succeed because the second level cache should have the
			// entity to load, allowing the session to not use the connection at all.
			// Will fail if a transaction manager tries to enlist user supplied connection. Do
			// not add a transaction scope below.
			using (var s = Sfi.WithOptions().Connection(connection).OpenSession())
			{
				CacheablePerson person = null;
				Assert.DoesNotThrow(() => person = s.Load<CacheablePerson>(id), "Failed loading entity from second level cache.");
				Assert.That(person.NotNullData, Is.EqualTo(notNullData));
			}
		}
	}

	public class SystemTransactionWithoutConnectionFromPrepareFixture : SystemTransactionFixture
	{
		protected override bool UseConnectionOnSystemTransactionPrepare => false;
	}
}