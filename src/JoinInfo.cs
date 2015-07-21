using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Automao.Data.Mapping;

namespace Automao.Data
{
	public class Join
	{
		private Join _parent;
		private List<Join> _children;
		private ClassInfo _host;

		public Join(Join parent, ClassInfo host)
		{
			_children = new List<Join>();
			_parent = parent;
			_host = host;
			host.Joins.Add(this);
		}

		public Join Parent
		{
			get
			{
				return _parent;
			}
		}

		public ClassInfo Host
		{
			get
			{
				return _host;
			}
		}

		public JoinPropertyNode JoinInfo
		{
			get;
			set;
		}

		public ClassInfo Target
		{
			get;
			set;
		}

		public int Index
		{
			get;
			set;
		}

		/// <summary>
		/// 一级一级往上，获取所有的Parent
		/// </summary>
		/// <param name="list"></param>
		/// <param name="stop"></param>
		public List<Join> GetParent(Func<Join, bool> stop)
		{
			var result = new List<Join>();

			if(this.Parent != null && !stop(this.Parent))
			{
				result.Add(this.Parent);
				result.AddRange(this.Parent.GetParent(stop));
			}

			return result;
		}

		public string ToJoinSql(bool caseSensitive)
		{
			return CreatJoinSql(caseSensitive, this);
		}

		public static string CreatJoinSql(bool caseSensitive, Join join)
		{
			return CreatJoinSql(caseSensitive, join, join.Host.AsName, join.JoinInfo.Member.ToDictionary(jc => jc.Key.Column, jc => jc.Value.Column));
		}

		public static string CreatJoinSql(bool caseSensitive, Join join, string hostAsName, Dictionary<string, string> relation)
		{
			var isLeftJoin = join.JoinInfo.Type == JoinType.Left;

			var joinformat = "{0} JOIN {1} {2} ON {3}";
			var onformat = caseSensitive ? "{0}.\"{1}\"={2}.\"{3}\"" : "{0}.{1}={2}.{3}";
			return string.Format(joinformat, isLeftJoin ? "LeFT" : "INNER", join.Target.ClassNode.GetTableName(caseSensitive), join.Target.AsName,
				string.Join(" AND ", relation.Select(jc => string.Format(onformat, hostAsName, jc.Key, join.Target.AsName, jc.Value))));
		}
	}

	//public class JoinTree
	//{
	//	#region 字段
	//	private JoinTree _parent;
	//	private string _tableEx;
	//	private ClassNode _classInfo;
	//	private List<JoinTree> _children;
	//	private JoinClassPropertyInfo _currentJoinColumn;
	//	#endregion

	//	#region 构造函数
	//	public JoinTree(JoinTree parent, string tableEx, JoinClassPropertyInfo joinColumn)
	//	{
	//		_parent = parent;
	//		_tableEx = tableEx;
	//		_classInfo = joinColumn.Host;
	//		_currentJoinColumn = joinColumn;
	//		_children = new List<JoinTree>();
	//		if(parent != null)
	//		{
	//			_parent._children.Add(this);
	//		}
	//	}
	//	#endregion

	//	#region 属性
	//	public JoinTree Parent
	//	{
	//		get
	//		{
	//			return _parent;
	//		}
	//	}

	//	public string Property
	//	{
	//		get
	//		{
	//			return _currentJoinColumn.Name;
	//		}
	//	}

	//	public string TableEx
	//	{
	//		get
	//		{
	//			return _tableEx;
	//		}
	//	}

	//	public ClassNode ClassInfo
	//	{
	//		get
	//		{
	//			return _classInfo;
	//		}
	//	}

	//	public JoinClassPropertyInfo ParentJoinColumn
	//	{
	//		get
	//		{
	//			if(_parent == null)
	//				return null;

	//			return _parent._currentJoinColumn;
	//		}
	//	}

	//	public List<JoinTree> Children
	//	{
	//		get
	//		{
	//			return _children;
	//		}
	//	}
	//	#endregion

	//	#region 方法
	//	/// <summary>
	//	/// 一级一级往上，获取所有的Parent
	//	/// </summary>
	//	/// <param name="list"></param>
	//	/// <param name="stop"></param>
	//	public List<JoinTree> GetParent(Func<JoinTree, bool> stop)
	//	{
	//		var result = new List<JoinTree>();

	//		if(!stop(this._parent) && this._parent != null)
	//		{
	//			result.Add(this._parent);
	//			result.AddRange(this._parent.GetParent(stop));
	//		}

	//		return result;
	//	}

	//	public string ToJoinSql(bool caseSensitive)
	//	{
	//		return CreatJoinSql(_parent._currentJoinColumn, caseSensitive, _tableEx, _parent._tableEx);
	//	}

	//	public static string CreatJoinSql(JoinClassPropertyInfo parentJoinColumn, bool caseSensitive, string joinTableNameEx, string parentTableNameEx)
	//	{
	//		var isLeftJoin = parentJoinColumn.Type == JoinType.Left;
	//		var joinTableName = parentJoinColumn.Target.Table;

	//		return CreatJoinSql(caseSensitive, isLeftJoin, joinTableName, joinTableNameEx, parentTableNameEx, parentJoinColumn.Relation.ToDictionary(p => p.Key.Column, p => p.Value.Column));
	//	}

	//	public static string CreatJoinSql(bool caseSensitive, bool isLeftJoin, string joinTableName, string joinTableNameEx, string parentTableNameEx, Dictionary<string, string> joinColumns)
	//	{
	//		var joinformat = caseSensitive ? "{0} JOIN \"{1}\" {2} ON {3}" : "{0} JOIN {1} {2} ON {3}";
	//		var onformat = caseSensitive ? "{0}.\"{1}\"={2}.\"{3}\"" : "{0}.{1}={2}.{3}";
	//		return string.Format(joinformat, isLeftJoin ? "LeFT" : "INNER", joinTableName, joinTableNameEx,
	//			string.Join(" AND ", joinColumns.Select(jc => string.Format(onformat, parentTableNameEx, jc.Key, joinTableNameEx, jc.Value))));
	//	}
	//	#endregion
	//}
}
