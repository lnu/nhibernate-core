using NHibernate.Engine;
using NHibernate.Test.TransactionTest;

namespace NHibernate.Test.SystemTransactions
{
	public abstract class SystemTransactionFixtureBase : TransactionFixtureBase
	{
		protected override bool AppliesTo(ISessionFactoryImplementor factory)
			=> factory.ConnectionProvider.Driver.SupportsSystemTransactions && base.AppliesTo(factory);
	}
}