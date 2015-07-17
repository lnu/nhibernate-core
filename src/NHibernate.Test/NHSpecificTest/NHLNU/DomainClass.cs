using System.Collections.Generic;
using System;
namespace NHibernate.Test.NHSpecificTest.NHLNU
{
	public abstract class BaseClass
	{
		public virtual Guid Id { get; set; }
		public virtual System.Type GetUnproxiedType()
		{
			return this.GetType();
		}
	}

	public class Job : BaseClass
	{
		public virtual IList<JobMaterial> JobMaterials { get; set; }
		public Job()
		{
			JobMaterials = new List<JobMaterial>();
		}
	}

	public class JobTranslation : Job
	{
		public virtual string PropertyA { get; set; }
	}

	public class JobModification : Job
	{
		public virtual string PropertyB { get; set; }
	}

	public class JobRevision : Job
	{
		public virtual string PropertyC { get; set; }
	}

	public class JobMaterial : BaseClass
	{
		public virtual Job Job { get; set; }
		public virtual Material Material { get; set; }
	}

	public class Material : BaseClass
	{
		public virtual MaterialType MaterialType { get; set; }
	}

	public class PhysicalFile : Material
	{
		public virtual int FileSize { get; set; }
		public virtual string FileName { get; set; }
		public virtual string PhysicalPath { get; set; }
	}

	public class NetworkFile : Material
	{
		public virtual string FilePath { get; set; }
	}

	public class Url : Material
	{
		public virtual string UrlName { get; set; }
		public virtual string ShortName { get; set; }
		public virtual bool IsSecure { get; set; }
		public virtual string Login { get; set; }
		public virtual string Password { get; set; }
	}

	public enum MaterialType
	{
		PhysicalFile = 0,
		Url = 1,
		NetworkFile = 2
	}
}