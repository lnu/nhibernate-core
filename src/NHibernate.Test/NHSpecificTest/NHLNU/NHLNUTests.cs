using NUnit.Framework;
using NHibernate.Criterion;
using NHibernate.Dialect;
using NHibernate.Linq;
using System.Linq;
using System;
using System.Data;

namespace NHibernate.Test.NHSpecificTest.NHLNU
{
	[TestFixture]
	public class NHLNUTests : BugTestCase
	{
		protected override void OnSetUp()
		{
			base.OnSetUp();
			using (ISession session = this.OpenSession())
			{
				session.BeginTransaction();
				System.Random r = new Random(5);
				// create 200 jobs with 10 materials of different types
				for (int i = 0; i < 300; i++)
				{
					var randomNumber = r.Next(0, 3);
					Job job = null;
					switch (randomNumber)
					{
						case 0:
							job = new JobTranslation() { PropertyA = "TestTrnaslation" + i.ToString() };
							break;
						case 1:
							job = new JobModification() { PropertyB = "TestModification" + i.ToString() };
							break;
						case 2:
							job = new JobRevision() { PropertyC = "TestRevision" + i.ToString() };
							break;
					}

					for (var j = 0; j < 15; j++)
					{
						randomNumber = r.Next(0, 3);
						Material m = null;
						switch (randomNumber)
						{
							case 0:
								m = new PhysicalFile() { FileName = "TestFileName" + j, FileSize = 100 + j, MaterialType = MaterialType.PhysicalFile, PhysicalPath = "c:\\" + j.ToString() + ".txt" };
								break;
							case 1:
								m = new NetworkFile() { FilePath = "\\\\xxx\\" + j.ToString() + ".txt", MaterialType = MaterialType.NetworkFile };
								break;
							case 2:
								m = new Url() { IsSecure = false, Login = null, Password = null, ShortName = "google" + j.ToString(), UrlName = "http://www.google.be/" + j.ToString(), MaterialType = MaterialType.Url };
								break;
						}
						job.JobMaterials.Add(new JobMaterial() { Job = job, Material = m });
					}
					session.Save(job);
				}
				session.Flush();
				session.Transaction.Commit();
				session.Clear();
			}
		}

		protected override void DropSchema()
		{
			base.DropSchema();
		}

		protected override void OnTearDown()
		{
			base.OnTearDown();
			using (ISession session = this.OpenSession())
			{
				session.Delete("from JobMaterial");
				session.Delete("from JobTranslation");
				session.Delete("from JobModification");
				session.Delete("from JobRevision");
				session.Delete("from PhysicalFile");
				session.Delete("from Url");
				session.Delete("from NetworkFile");
				session.Flush();
			}
		}

		protected override bool AppliesTo(NHibernate.Dialect.Dialect dialect)
		{
			return true;// dialect as Oracle10gDialect != null;
		}

		[Test]
		public void TestIfRightEntityTypeLoaded()
		{
			Console.WriteLine("Starting test {0}", DateTime.Now);
			using (ISession session = this.OpenSession())
			{
				IDbCommand com= session.Connection.CreateCommand();
				com.CommandText = "alter session  set optimizer_features_enable='11.2.0.3'";
				com.CommandType = CommandType.Text;
				com.ExecuteNonQuery();
				//com.CommandText = "alter session  set optimizer_adaptive_features=false";
				//com.ExecuteNonQuery();
				
				//session.CreateSQLQuery("alter session  set optimizer_features_enable='11.2.0.3'").ExecuteUpdate();
				//session.CreateSQLQuery("alter session  set optimizer_adaptive_features=false").ExecuteUpdate();				
				using (var transaction = session.BeginTransaction())
				{
					var jobs = session.Query<Job>().ToList();
					Console.WriteLine(jobs.Count);
					foreach (var x in jobs)
					{
						Console.WriteLine("{0} JobMaterials", x.JobMaterials.Count);
						foreach (var q in x.JobMaterials)
						{
							var jm = q.Material.MaterialType;
							Console.WriteLine("Job of type:{0} with id:{1} type:{2} {3}", x.GetType().Name, x.Id, jm, q.Material.GetUnproxiedType().Name);
							switch (q.Material.MaterialType)
							{
								case MaterialType.PhysicalFile:
									Assert.IsTrue(q.Material.GetUnproxiedType() == typeof(PhysicalFile));
									break;
								case MaterialType.Url:
									Assert.IsTrue(q.Material.GetUnproxiedType() == typeof(Url));
									break;
								case MaterialType.NetworkFile:
									Assert.IsTrue(q.Material.GetUnproxiedType() == typeof(NetworkFile));
									break;
							}
						}
					}
					transaction.Rollback();
				}
			}
			Console.WriteLine("Test ended {0}", DateTime.Now);
		}
	}
}
