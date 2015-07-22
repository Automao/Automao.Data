using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Automao.Data.Mapping;

namespace Automao.Data
{
	public class ClassInfo
	{
		#region 字段
		private string _asName;
		private string _as;
		private int _asIndex;
		private Mapping.ClassNode _classNode;
		private List<Join> _joins; 
		#endregion

		#region 构造函数
		public ClassInfo(string @as, ClassNode classNode)
		{
			_as = @as;
			_classNode = classNode;
			_joins = new List<Join>();

			_asName = @as;
		} 
		#endregion

		#region 属性
		public ClassNode ClassNode
		{
			get
			{
				return _classNode;
			}
		}

		public List<Join> Joins
		{
			get
			{
				return _joins;
			}
		}

		public string AsName
		{
			get
			{
				return _asName;
			}
		}

		public string As
		{
			get
			{
				return _as;
			}
		}

		public int AsIndex
		{
			get
			{
				return _asIndex;
			}
		} 
		#endregion

		#region 方法
		public void SetIndex(int index)
		{
			_asIndex = index;
			if(index > 0)
				_asName += index.ToString();
		}

		public int SetJoinIndex(int start)
		{
			foreach(var join in _joins)
			{
				join.Target.SetIndex(start);
				start = join.Target.SetJoinIndex(start + 1);
			}

			return start;
		}

		public string GetTableName(bool caseSensitive)
		{
			return string.Format("{0} {1}", _classNode.GetTableName(caseSensitive), _asName);
		}
		#endregion
	}
}
