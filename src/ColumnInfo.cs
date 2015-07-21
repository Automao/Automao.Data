using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Automao.Data.Mapping;

namespace Automao.Data
{
	public class ColumnInfo
	{
		#region 字段
		private string _original;
		private string _field;
		private ClassInfo _classInfo;
		private Join _join;
		private PropertyNode _propertyNode;
		private string _aggregateFunctionName;//聚合函数名称
		private string _selectColumn;
		private string _columnEx;
		private string _subAsName;
		#endregion

		#region 构造函数
		public ColumnInfo(string original)
		{
			_original = original;
		}
		#endregion

		#region 属性
		/// <summary>
		/// 原文
		/// </summary>
		public string Original
		{
			get
			{
				return _original;
			}
		}

		public string Field
		{
			get
			{
				return _field;
			}
		}

		public ClassInfo ClassInfo
		{
			get
			{
				return _classInfo;
			}
		}

		public PropertyNode PropertyNode
		{
			get
			{
				if(_propertyNode == null)
					_propertyNode = _classInfo.ClassNode.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(_field, System.StringComparison.OrdinalIgnoreCase));
				return _propertyNode;
			}
		}

		public Join Join
		{
			get
			{
				return _join;
			}
		}
		#endregion

		#region 方法
		public string ToColumn(bool caseSensitive)
		{
			var columnformat = caseSensitive ? "{0}.\"{1}\"" : "{0}.{1}";
			var field = _propertyNode != null ? _propertyNode.Column : _field;

			if(field.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				return "COUNT(0)";
			else if(!string.IsNullOrEmpty(_aggregateFunctionName))
				return string.Format(caseSensitive ? "{0}({1}.\"{2}\")" : "{0}({1}.{2})", _aggregateFunctionName.ToUpper(), _classInfo.AsName, field);

			return string.Format(columnformat, _classInfo.AsName, field);
		}

		public string ToSelectColumn(bool caseSensitive)
		{
			if(!string.IsNullOrEmpty(_selectColumn))
				return _selectColumn;

			var columnformat = caseSensitive ? "{0}.\"{1}\"" : "{0}.{1}";
			var field = _propertyNode != null ? _propertyNode.Column : _field;
			if(field.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				return "COUNT(0)";
			else if(!string.IsNullOrEmpty(_aggregateFunctionName))
				return string.Format(caseSensitive ? "{0}({1}.\"{2}\")" : "{0}({1}.{2})", _aggregateFunctionName.ToUpper(), _classInfo.AsName, field);

			_selectColumn = string.Format(columnformat, _classInfo.AsName, field) + " " + GetColumnEx(caseSensitive);
			return _selectColumn;
		}

		public string GetColumnEx(bool caseSensitive)
		{
			if(!string.IsNullOrEmpty(_columnEx))
				return _columnEx;

			var asName = string.IsNullOrEmpty(_subAsName) ? _classInfo.AsName : _subAsName;

			var columnformat = caseSensitive ? "\"{0}_{1}\"" : "{0}_{1}";
			var field = _propertyNode != null ? _propertyNode.Column : _field;
			if(field.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				return string.Format(caseSensitive ? "\"{0}_Count\"" : "{0}_Count", asName);
			else if(!string.IsNullOrEmpty(_aggregateFunctionName))
				return string.Format(caseSensitive ? "\"{1}_{0}_{2}\"" : "{1}_{0}_{2}", _aggregateFunctionName.ToUpper(), asName, field);

			_columnEx = string.Format(columnformat, asName, field);
			return _columnEx;
		}
		#endregion

		#region override object
		public override int GetHashCode()
		{
			return _original.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			return object.Equals(obj, _original);
		}
		#endregion

		#region 静态方法
		public static Dictionary<string, ColumnInfo> Create(IEnumerable<string> columns, ClassInfo root, int joinStartIndex, out List<Join> joinList)
		{
			var result = new Dictionary<string, ColumnInfo>();
			var joinDic = new Dictionary<string, Join>();

			foreach(var item in columns)
			{
				if(result.ContainsKey(item))
					continue;
				var columnInfo = new ColumnInfo(item);

				var temp = item;
				if(!temp.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				{
					var match = Regex.Match(item, @"(?'name'.+)\((?'property'.+)\)");
					if(match.Success)
					{
						temp = match.Groups["property"].Value;
						columnInfo._aggregateFunctionName = match.Groups["name"].Value;
					}
				}

				var array = temp.Split('.');
				if(array.Length > 0)
				{
					var key = string.Empty;
					var host = root;
					Join parent = null;
					foreach(var property in array)
					{
						var joinInfo = host.ClassNode.JoinList.FirstOrDefault(p => p.Name.Equals(property, System.StringComparison.OrdinalIgnoreCase));
						if(joinInfo == null)//不是导航属性，就当它是普通属性
						{
							List<JoinPropertyNode> parents;
							columnInfo._classInfo = host;
							columnInfo._field = property;

							if(IsParentProperty(host.ClassNode, property, out parents))//搞定继承的问题
							{
								parents.Reverse();
								var last = AddJoin(joinDic, parent, host, ref joinStartIndex, parents[0].Name, parents);
								columnInfo._join = last;
								columnInfo._subAsName = host.AsName;
								columnInfo._classInfo = last.Target;
							}
							break;
						}

						if(string.IsNullOrEmpty(key))
							key = property;
						else
							key += "." + property;

						if(!joinDic.ContainsKey(key))
						{
							var join = new Join(parent, host);
							join.Index = joinStartIndex;
							join.JoinInfo = joinInfo;
							join.Target = new ClassInfo("J", joinStartIndex++, joinInfo.Target);

							host = join.Target;
							parent = join;
							columnInfo._join = join;

							joinDic.Add(key, join);
						}
						else
						{
							var join = joinDic[key];

							columnInfo._join = join;
							parent = join;
							host = join.Target;
						}
					}
				}
				else
				{
					columnInfo._classInfo = root;
					columnInfo._field = temp;
				}

				result.Add(item, columnInfo);
			}

			joinList = joinDic.Values.ToList();
			return result;
		}

		public static bool IsParentProperty(ClassNode host, string property, out List<JoinPropertyNode> parents)
		{
			parents = null;
			if(host.BaseClassNode == null)
				return false;

			if(host.PropertyNodeList.Any(p => p.Name.Equals(property, System.StringComparison.OrdinalIgnoreCase)))
				return false;

			var propertyInfo = host.EntityType.GetProperty(property, System.Reflection.BindingFlags.DeclaredOnly | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
			if(propertyInfo != null)
			{
				var node = new PropertyNode(property);
				host.PropertyNodeList.Add(node);
				return false;
			}

			foreach(var join in host.JoinList)
			{
				if(host.EntityType.IsSubclassOf(join.Target.EntityType))
				{
					IsParentProperty(join.Target, property, out parents);
					if(parents == null)
						parents = new List<JoinPropertyNode>();
					parents.Add(join);
					return true;
				}
			}

			return false;
		}

		public static Join AddJoin(Dictionary<string, Join> list, Join parent, ClassInfo host, ref int joinStartIndex, string key, IEnumerable<JoinPropertyNode> values)
		{
			Join join = null;
			foreach(var value in values)
			{
				if(list.ContainsKey(key))
					join = list[key];
				else
				{
					join = new Join(parent, host);
					join.Index = joinStartIndex;
					join.JoinInfo = value;
					join.Target = new ClassInfo("J", joinStartIndex++, join.JoinInfo.Target);
					list.Add(key, join);
				}

				host = join.Target;
				parent = join;
				key += "." + value.Name;
			}

			return join;
		}
		#endregion
	}
}
