using System;
using System.Collections.Generic;

namespace NHibernate.Test.NHSpecificTest.InheritanceBug
{
	public class Entity
	{
		public virtual Guid Id { get; set; }
		public virtual string Name { get; set; }
		public virtual BaseClass BaseClass { get; set; }
	}

	public abstract class BaseClass
	{
		public virtual Guid Id { get; set; }
	}

	public class Child1 : BaseClass
	{
		public virtual string Child1Name { get; set; }
		public virtual Common Common { get; set; }
	}

	public class Child2 : BaseClass
	{
		public virtual string Child2Name { get; set; }
		public virtual Common Common { get; set; }
	}

	public class Child3 : BaseClass
	{
		public virtual string Child3Name { get; set; }
		public virtual int OtherProperty { get; set; }
	}

	public class Common
	{
		public virtual Guid Id { get; set; }
		public virtual string CommonName { get; set; }
	}
}