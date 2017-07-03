using System;
using System.Collections;
using NHibernate.AdoNet;
using NHibernate.Engine;
using NHibernate.Engine.Transaction;
using NHibernate.Transaction;

namespace NHibernate.Test.NHSpecificTest.NH1054
{
	public class DummyTransactionFactory : ITransactionFactory
	{
		public void Configure(IDictionary props)
		{
		}

		public ITransaction CreateTransaction(ISessionImplementor session)
		{
			throw new NotImplementedException();
		}

		public void EnlistInSystemTransactionIfNeeded(ISessionImplementor session)
		{
			throw new NotImplementedException();
		}

		public bool IsInActiveSystemTransaction(ISessionImplementor session)
		{
			return false;
		}

		public void ExecuteWorkInIsolation(ISessionImplementor session, IIsolatedWork work, bool transacted)
		{
			throw new NotImplementedException();
		}
	}
}
