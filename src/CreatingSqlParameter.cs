using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zongsoft.Data;

namespace Automao.Data
{
	/// <summary>
	/// 创建sql的参数
	/// </summary>
	public class CreatingSqlParameter
	{
		#region 字段
		private bool _subquery;
		private int _tableIndex;
		private int _joinStartIndex;
		private int _valueIndex;
		private Zongsoft.Data.ConditionOperator _conditionOperator;
		#endregion

		#region 构造函数
		public CreatingSqlParameter(bool subquery, int tableIndex, int joinStartIndex, int valueIndex)
		{
			_subquery = subquery;
			_tableIndex = tableIndex;
			_joinStartIndex = joinStartIndex;
			_valueIndex = valueIndex;
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

		public int TableIndex
		{
			get
			{
				return _tableIndex;
			}
			set
			{
				_tableIndex = value;
				if(SyncModel != null)
					SyncModel._tableIndex = value;
			}
		}

		public int JoinStartIndex
		{
			get
			{
				return _joinStartIndex;
			}
			set
			{
				_joinStartIndex = value;
				if(SyncModel != null)
					SyncModel._joinStartIndex = value;
			}
		}

		public int ValueIndex
		{
			get
			{
				return _valueIndex;
			}
			set
			{
				_valueIndex = value;
				if(SyncModel != null)
					SyncModel._valueIndex = value;
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

		protected CreatingSqlParameter SyncModel
		{
			get;
			set;
		}
		#endregion
	}
}
