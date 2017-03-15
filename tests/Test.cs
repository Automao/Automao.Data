using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

using Zongsoft.Data;
using Automao.Data;

namespace Automao.Data.Tests
{
	public class Test
	{
		private Automao.Data.MySql.MySqlDataAccess _db;

		[Xunit.Fact(DisplayName = "select")]
		public void TestSelect()
		{
			/*
			var data = _db.Select<Automao.Common.Models.VehicleModel>("VehicleModel",
				new Condition("Series.Group.Brand.BrandCode", "北%"),
				m => new
				{
					m,
					m.Series,
					m.Series.Group,
					m.Series.Group.Brand
				},
				sorting: Sorting.Ascending("Series.Group.Brand.Name"));
			var result = data.ToArray();

			Xunit.Assert.NotEmpty(result);
			*/
		}

		[Xunit.Fact(DisplayName = "insert")]
		public void TestInsert()
		{
			/*
			var department = new Common.Models.Department();
			department.Address = "address";
			department.ContactId = 1001;
			department.CorporationId = 1000;
			department.DepartmentId = 1;
			department.Name = "name";
			department.ParentId = 0;
			department.PhoneNumber = "110";
			department.PrincipalId = 1002;
			department.Remark = "Remark";

			var result = _db.Insert<Automao.Common.Models.Department>("Department", department);
			Xunit.Assert.True(result > 0);
			*/
		}

		[Xunit.Fact(DisplayName = "delete")]
		public void TestDelete()
		{
			//var result = _db.Delete<Automao.Common.Models.Department>("Department", new ConditionCollection(ConditionCombine.And) { new Condition("DepartmentId", 1), new Condition("CorporationId", 1000) }, null);
			//Xunit.Assert.True(result > 0);
		}

		[Xunit.Fact(DisplayName = "update")]
		public void TestUpdate()
		{
			/*
			var department = new Common.Models.Department();
			department.Address = "address" + DateTime.Now.Second;
			department.ContactId = 1001;
			department.CorporationId = 1000;
			department.DepartmentId = 2;
			department.Name = "name" + DateTime.Now.Second;
			department.ParentId = 0;
			department.PhoneNumber = "110" + DateTime.Now.Second;
			department.PrincipalId = 1002;
			department.Remark = "Remark" + DateTime.Now.Second;

			var result = _db.Update<Automao.Common.Models.Department>("Department", department, new Condition("DepartmentId", 2), "-Name,-PhoneNumber");
			Xunit.Assert.True(result > 0);
			*/
		}
	}
}
