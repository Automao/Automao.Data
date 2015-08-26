using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zongsoft.Data;

namespace Automao.Data
{
	/// <summary>
	/// 创建查询语句的参数
	/// </summary>
	public class CreatingSelectSqlParameter : CreatingSqlParameter
	{
		#region 构造函数
		public CreatingSelectSqlParameter(bool subquery, int tableIndex, int joinStartIndex, int valueIndex)
			: base(subquery, tableIndex, joinStartIndex, valueIndex)
		{
		}
		#endregion

		#region 属性
		public ClassInfo ClassInfo
		{
			get;
			set;
		}

		public ICondition Condition
		{
			get;
			set;
		}
		public string[] Members
		{
			get;
			set;
		}

		public string[] ConditionNames
		{
			get;
			set;
		}

		public Dictionary<string, ColumnInfo> AllColumnInfos
		{
			get;
			set;
		}

		public Paging Paging
		{
			get;
			set;
		}

		public Grouping Grouping
		{
			get;
			set;
		}

		public Sorting[] Sorting
		{
			get;
			set;
		}
		#endregion
	}
}
