using NHibernate.Cfg;
using NHibernate.Engine;
using NHibernate.Test.TransactionTest;
using NUnit.Framework;

namespace NHibernate.Test.SystemTransactions
{
	public abstract class SystemTransactionFixtureBase : TransactionFixtureBase
	{
		protected override bool AppliesTo(ISessionFactoryImplementor factory)
			=> factory.ConnectionProvider.Driver.SupportsSystemTransactions && base.AppliesTo(factory);

		protected abstract bool UseConnectionOnSystemTransactionEvents { get; }

		protected override void Configure(Configuration configuration)
		{
			base.Configure(configuration);
			configuration
				.SetProperty(
					Environment.UseConnectionOnSystemTransactionEvents,
					UseConnectionOnSystemTransactionEvents.ToString());
		}

		protected void IgnoreIfUnsupported(bool explicitFlush)
		{
			Assume.That(
				new[] { explicitFlush, UseConnectionOnSystemTransactionEvents },
				Has.Some.EqualTo(true),
				"Implicit flush cannot work without using connection from system transaction prepare phase");
		}
	}
}