using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using NHibernate.Linq;
using NUnit.Framework;
using NHibernate.Mapping.ByCode;
using NHibernate.Cfg.MappingSchema;

namespace NHibernate.Test.NHSpecificTest.InheritanceBug
{
	[TestFixture]
	public class Fixture : TestCaseMappingByCode
	{
		protected override HbmMapping GetMappings()
		{
			var mapper = new ModelMapper();
			mapper.Class<Entity>(rc =>
			{
				rc.Id(x => x.Id, m => m.Generator(Generators.GuidComb));
				rc.Property(x => x.Name);
				rc.ManyToOne(p => p.BaseClass, map => map.Cascade(Mapping.ByCode.Cascade.All));
			});
			mapper.Class<BaseClass>(rc =>
			{
				rc.Id(x => x.Id, m => m.Generator(Generators.GuidComb));
			});
			mapper.JoinedSubclass<Child1>(rc =>
			{
				rc.Property(p => p.Child1Name);
				rc.ManyToOne(p => p.Common, map => map.Cascade(Mapping.ByCode.Cascade.All));
			});
			mapper.JoinedSubclass<Child2>(rc =>
			{
				rc.Property(p => p.Child2Name);
				rc.ManyToOne(p => p.Common, map => map.Cascade(Mapping.ByCode.Cascade.All));
			});
			mapper.JoinedSubclass<Child3>(rc =>
			{
				rc.Property(p => p.Child3Name);
				rc.Property(p => p.OtherProperty);
			});
			mapper.Class<Common>(rc =>
			{
				rc.Id(x => x.Id, m => m.Generator(Generators.GuidComb));
				rc.Property(x => x.CommonName);
			});

			return mapper.CompileMappingForAllExplicitlyAddedEntities();
		}

		protected override void OnSetUp()
		{
			using (ISession session = OpenSession())
			using (ITransaction transaction = session.BeginTransaction())
			{
				var common1 = new Common()
				{
					CommonName = "Common1"
				};
				var common2 = new Common()
				{
					CommonName = "Common2"
				};
				var child1 = new Child1()
				{
					Child1Name = "Child1",
					Common = common1
				};
				var child2 = new Child2()
				{
					Child2Name = "Child2",
					Common = common2
				};
				var child3 = new Child2()
				{
					Child2Name = "Child3",
					Common = common2
				};
				var entity1 = new Entity()
				{
					Name = "entity1",
					BaseClass = child1
				};
				var entity2 = new Entity()
				{
					Name = "entity2",
					BaseClass = child2
				};
				var entity3 = new Entity()
				{
					Name = "entity3",
					BaseClass = child3
				};
				session.Save(child1);
				session.Save(child2);
				session.Save(child3);
				session.Save(entity1);
				session.Save(entity2);
				session.Save(entity3);
				session.Flush();
				transaction.Commit();
			}
		}

		protected override void OnTearDown()
		{
			using (ISession session = OpenSession())
			using (ITransaction transaction = session.BeginTransaction())
			{
				session.Delete("from Entity");

				session.Flush();
				transaction.Commit();
			}
		}

		[Test]
		public void InheritanceIssueWithLinq()
		{
			using (ISession session = OpenSession())
			using (session.BeginTransaction())
			{
				var result = session.Query<Entity>().Where(p => ((Child2)p.BaseClass).Common.CommonName == "Common2").Count();
				Assert.AreEqual(1, result);
			}
		}
	}
}