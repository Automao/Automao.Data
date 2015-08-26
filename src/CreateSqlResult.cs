using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automao.Data
{
	public class CreateSqlResult
	{
		#region 字段
		private string _sql;
		private object[] _values; 
		#endregion

		#region 构造函数
		public CreateSqlResult(string sql, object[] values)
		{
			_sql = sql;
			_values = values;
		} 
		#endregion

		#region 属性
		public string Sql
		{
			get
			{
				return _sql;
			}
		}

		public object[] Values
		{
			get
			{
				return _values;
			}
		} 
		#endregion
	}
}
