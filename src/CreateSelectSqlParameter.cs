using System.Collections.Generic;
using Zongsoft.Data;

namespace Automao.Data
{
	/// <summary>
	/// 子类创建查询语句的参数
	/// </summary>
	public class CreateSelectSqlParameter
	{
		#region 字段
		private bool _subquery;
		private ClassInfo _info;
		private List<ColumnInfo> _columns;
		private string _where;
		private string _join;
		private string _orderby;
		private Paging _paging;
		private Zongsoft.Data.ConditionOperator _conditionOperator;
		#endregion

		#region 构造函数
		/// <summary>
		/// 
		/// </summary>
		/// <param name="subquery">是否是子查询</param>
		public CreateSelectSqlParameter(bool subquery)
		{
			_subquery = subquery;
		}
		#endregion

		#region 属性
		public bool Subquery
		{
			get
			{
				return _subquery;
			}
			set
			{
				_subquery = value;
			}
		}

		public ClassInfo Info
		{
			get
			{
				return _info;
			}
			set
			{
				_info = value;
			}
		}

		public List<ColumnInfo> Columns
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

		public string Where
		{
			get
			{
				return _where;
			}
			set
			{
				_where = value;
			}
		}

		public string Join
		{
			get
			{
				return _join;
			}
			set
			{
				_join = value;
			}
		}

		public string Orderby
		{
			get
			{
				return _orderby;
			}
			set
			{
				_orderby = value;
			}
		}

		public Paging Paging
		{
			get
			{
				return _paging;
			}
			set
			{
				_paging = value;
			}
		}

		/// <summary>
		/// 创建的sql用于子查询时使用的ConditionOperator
		/// </summary>
		public ConditionOperator ConditionOperator
		{
			get
			{
				return _conditionOperator;
			}
			set
			{
				_conditionOperator = value;
			}
		}
		#endregion
	}
}
