using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Automao.Data
{
	public class ColumnInfo
	{
		#region 字段
		private string _original;
		private string _propertyLink;
		private string _field;
		private ClassInfo _classInfo;
		private ClassPropertyInfo _propertyInfo;
		private ClassInfo _rootInfo;
		private JoinInfo _joinInfo;
		private Dictionary<string, JoinInfo> _joinInfoList;
		private JoinInfo _rootJoin;
		private string _aggregateFunctionName;//聚合函数名称
		private string _selectColumn;
		private string _columnEx;
		#endregion

		#region 构造函数
		public ColumnInfo(int joinStartIndex, string original, ClassInfo rootInfo, Dictionary<string, JoinInfo> joinInfoList, JoinInfo rootJoin)
		{
			_original = original;
			_rootInfo = rootInfo;
			_joinInfoList = joinInfoList;
			_rootJoin = rootJoin;

			Init(joinStartIndex);
		}
		#endregion

		#region 属性
		public string Original
		{
			get
			{
				return _original;
			}
		}

		public string PropertyLink
		{
			get
			{
				return _propertyLink;
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

		public ClassPropertyInfo PropertyInfo
		{
			get
			{
				return _propertyInfo;
			}
		}

		public JoinInfo JoinInfo
		{
			get
			{
				return _joinInfo;
			}
		}
		#endregion

		#region 私有方法
		private void Init(int joinStartIndex)
		{
			_classInfo = _rootInfo;
			var temp = _original;
			if(!temp.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
			{
				var match = Regex.Match(_original, @"(?'name'.+)\((?'property'.+)\)");
				if(match.Success)
				{
					temp = match.Groups["property"].Value;
					_aggregateFunctionName = match.Groups["name"].Value;
				}
			}

			var index = temp.LastIndexOf('.');

			if(index > 0)
			{
				_propertyLink = temp.Substring(0, index);
				_field = temp.Substring(index + 1);
				var list = _propertyLink.Split('.');

				for(int i = 0; i < list.Length; i++)
				{
					var key = string.Join(".", list.Take(i + 1));
					_classInfo = _classInfo.PropertyInfoList.FirstOrDefault(p => p.IsFKColumn && p.SetClassPropertyName == list[i]).Join;

					if(!_joinInfoList.ContainsKey(key))
					{
						var parent = i == 0 ? _rootJoin : _joinInfoList[string.Join(".", list.Take(i))];
						var joinColumns = parent.ClassInfo.PropertyInfoList.Where(p => p.IsFKColumn && p.SetClassPropertyName == list[i]).ToArray();

						var joinInfo = new JoinInfo(parent, list[i], "J" + (_joinInfoList.Count + joinStartIndex), _classInfo, joinColumns);
						_joinInfoList.Add(key, joinInfo);
					}
				}

				if(_joinInfoList.ContainsKey(_propertyLink))
					_joinInfo = _joinInfoList[_propertyLink];
			}
			else
			{
				_joinInfo = _rootJoin;
				_field = temp;
			}

			joinStartIndex += _joinInfoList.Count;

			_propertyInfo = _classInfo.PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName == _field);
		}
		#endregion

		#region 方法
		public string ToColumn(bool caseSensitive)
		{
			var columnformat = caseSensitive ? "{0}.\"{1}\"" : "{0}.{1}";
			var field = _propertyInfo != null ? _propertyInfo.TableColumnName : _field;

			if(field.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				return "COUNT(0)";
			else if(!string.IsNullOrEmpty(_aggregateFunctionName))
				return string.Format(caseSensitive ? "{0}({1}.\"{2}\")" : "{0}({1}.{2})", _aggregateFunctionName.ToUpper(), _joinInfo.TableEx, field);

			return string.Format(columnformat, _joinInfo.TableEx, field);
		}

		public string ToSelectColumn(bool caseSensitive)
		{
			if(!string.IsNullOrEmpty(_selectColumn))
				return _selectColumn;

			var columnformat = caseSensitive ? "{0}.\"{1}\"" : "{0}.{1}";
			var field = _propertyInfo != null ? _propertyInfo.TableColumnName : _field;
			if(field.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				return "COUNT(0)";
			else if(!string.IsNullOrEmpty(_aggregateFunctionName))
				return string.Format(caseSensitive ? "{0}({1}.\"{2}\")" : "{0}({1}.{2})", _aggregateFunctionName.ToUpper(), _joinInfo.TableEx, field);

			_selectColumn = string.Format(columnformat, _joinInfo.TableEx, field) + " " + GetColumnEx(caseSensitive);
			return _selectColumn;
		}

		public string GetColumnEx(bool caseSensitive)
		{
			if(!string.IsNullOrEmpty(_columnEx))
				return _columnEx;

			var columnformat = caseSensitive ? "\"{0}_{1}\"" : "{0}_{1}";
			var field = _propertyInfo != null ? _propertyInfo.TableColumnName : _field;
			if(field.Equals("count(0)", System.StringComparison.OrdinalIgnoreCase))
				return string.Format(caseSensitive ? "\"{0}_Count\"" : "{0}_Count", _joinInfo.TableEx);
			else if(!string.IsNullOrEmpty(_aggregateFunctionName))
				return string.Format(caseSensitive ? "\"{1}_{0}{2}\"" : "{1}_{0}{2}", _aggregateFunctionName.ToUpper(), _joinInfo.TableEx, field);

			_columnEx = string.Format(columnformat, _joinInfo.TableEx, field);
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
	}
}
