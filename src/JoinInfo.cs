using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automao.Data
{
	public class JoinInfo
	{
		#region 字段
		private JoinInfo _parent;
		private string _property;
		private string _tableEx;
		private ClassInfo _classInfo;
		private ClassPropertyInfo[] _parentJoinColumn;
		private List<JoinInfo> _children;
		#endregion

		#region 构造函数
		public JoinInfo(JoinInfo parent, string property, string tableEx, ClassInfo classInfo, ClassPropertyInfo[] parentJoinColumn)
		{
			_parent = parent;
			_property = property;
			_tableEx = tableEx;
			_classInfo = classInfo;
			_parentJoinColumn = parentJoinColumn;
			_children = new List<JoinInfo>();
			if(parent != null)
				_parent._children.Add(this);
		}
		#endregion

		#region 属性
		public JoinInfo Parent
		{
			get
			{
				return _parent;
			}
		}

		public string Property
		{
			get
			{
				return _property;
			}
		}

		public string TableEx
		{
			get
			{
				return _tableEx;
			}
		}

		public ClassInfo ClassInfo
		{
			get
			{
				return _classInfo;
			}
		}

		public ClassPropertyInfo[] ParentJoinColumn
		{
			get
			{
				return _parentJoinColumn;
			}
		}

		public List<JoinInfo> Children
		{
			get
			{
				return _children;
			}
		}
		#endregion

		#region 方法
		/// <summary>
		/// 一级一级往上，获取所有的Parent
		/// </summary>
		/// <param name="list"></param>
		/// <param name="stop"></param>
		public void GetParent(List<JoinInfo> list, Func<JoinInfo, bool> stop)
		{
			if(!stop(this._parent) && this._parent != null && !list.Contains(this._parent))
			{
				list.Add(this._parent);
				this._parent.GetParent(list, stop);
			}
		}

		public string ToJoinSql(bool caseSensitive)
		{
			return CreatJoinSql(caseSensitive, _parentJoinColumn.Any(p => p.Nullable), _classInfo.TableName, _tableEx, _parent._tableEx,
				_parentJoinColumn.ToDictionary(p => p.TableColumnName, p => p.JoinColumn.TableColumnName));
		}

		public static string CreatJoinSql(bool caseSensitive, bool isLeftJoin, string joinTableName, string joinTableNameEx, string parentTableNameEx, Dictionary<string, string> joinColumns)
		{
			var joinformat = caseSensitive ? "{0} JOIN \"{1}\" {2} ON {3}" : "{0} JOIN {1} {2} ON {3}";
			var onformat = caseSensitive ? "{0}.\"{1}\"={2}.\"{3}\"" : "{0}.{1}={2}.{3}";
			return string.Format(joinformat, isLeftJoin ? "LeFT" : "INNER", joinTableName, joinTableNameEx,
				string.Join(" AND ", joinColumns.Select(jc => string.Format(onformat, parentTableNameEx, jc.Key, joinTableNameEx, jc.Value))));
		}
		#endregion
	}
}
