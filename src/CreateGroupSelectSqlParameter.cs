using System.Collections.Generic;

namespace Automao.Data
{
	/// <summary>
	/// 子类创建要分组的查询语句的参数
	/// </summary>
	public class CreateGroupSelectSqlParameter : CreateSelectSqlParameter
	{
		#region 字段
		private string _newTableNameEx;
		private string _columns;
		private string _group;
		private string _having;
		private List<ColumnInfo> _groupedSelectColumns;
		private string _groupedJoin;
		#endregion

		#region 构造函数
		/// <summary>
		/// 
		/// </summary>
		/// <param name="subquery">是否是子查询</param>
		public CreateGroupSelectSqlParameter(bool subquery)
			: base(subquery)
		{
		}
		#endregion

		#region 属性
		public string NewTableNameEx
		{
			get
			{
				return _newTableNameEx;
			}
			set
			{
				_newTableNameEx = value;
			}
		}

		public string Columns1
		{
			get
			{
				return _columns;
			}
			set
			{
				_columns = value;
			}
		}

		public string Group
		{
			get
			{
				return _group;
			}
			set
			{
				_group = value;
			}
		}

		public string Having
		{
			get
			{
				return _having;
			}
			set
			{
				_having = value;
			}
		}

		public List<ColumnInfo> GroupedSelectColumns
		{
			get
			{
				return _groupedSelectColumns;
			}
			set
			{
				_groupedSelectColumns = value;
			}
		}

		public string GroupedJoin
		{
			get
			{
				return _groupedJoin;
			}
			set
			{
				_groupedJoin = value;
			}
		}
		#endregion
	}
}
