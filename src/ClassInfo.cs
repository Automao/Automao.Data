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
		private string _asName;
		private string _as;
		private int _asIndex;
		private Mapping.ClassNode _classNode;
		private List<Join> _joins;

		public ClassInfo(string @as, int asIndex, ClassNode classNode)
		{
			_as = @as;
			_classNode = classNode;
			_joins = new List<Join>();

			_asIndex = asIndex;
			_asName = @as;
			if(_asIndex > 0)
				_asName += _asIndex.ToString();
		}

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
	}
}
