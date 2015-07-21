using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Automao.Data.Mapping
{

	/// <summary>
	/// 属性详情
	/// </summary>
	public class PropertyNode
	{
		#region 字段
		private string _name;
		private string _column;
		private bool _isKey;
		private bool _unColumn;
		private bool _passedIntoConstructor;
		private string _constructorName;
		#endregion

		#region 构造函数
		public PropertyNode(string name)
		{
			_name = name;
		}
		#endregion

		#region 属性
		/// <summary>
		/// 类中属性名称
		/// </summary>
		public string Name
		{
			get
			{
				return _name;
			}
		}

		/// <summary>
		/// 是否是主键
		/// </summary>
		public bool IsKey
		{
			get
			{
				return _isKey;
			}
			set
			{
				_isKey = value;
			}
		}

		/// <summary>
		/// 不是一个列
		/// </summary>
		public bool UnColumn
		{
			get
			{
				return _unColumn;
			}
		}

		/// <summary>
		/// 表中列名
		/// </summary>
		public string Column
		{
			get
			{
				if(string.IsNullOrEmpty(_column))
					_column = Name;
				return _column;
			}
		}

		/// <summary>
		/// 是否传入构造函数
		/// </summary>
		public bool PassedIntoConstructor
		{
			get
			{
				return _passedIntoConstructor;
			}
		}

		/// <summary>
		/// 构造函数对应参数名称
		/// </summary>
		public string ConstructorName
		{
			get
			{
				return _constructorName;
			}
		}
		#endregion

		public static PropertyNode Create(XElement property)
		{
			string propertyInfoName = null, attribuleValue;

			if(MappingInfo.GetAttribuleValue(property, "name", out attribuleValue) || !(attribuleValue = property.Name.LocalName).Equals("property", StringComparison.OrdinalIgnoreCase))
				propertyInfoName = attribuleValue;

			var propertyInfo = new PropertyNode(propertyInfoName);

			if(MappingInfo.GetAttribuleValue(property, "constructorName", out attribuleValue))
			{
				propertyInfo._constructorName = attribuleValue;
				propertyInfo._passedIntoConstructor = true;
			}

			if(property.Attribute("column") == null)
				propertyInfo._column = propertyInfo.Name;
			else if(MappingInfo.GetAttribuleValue(property, "column", out attribuleValue))
				propertyInfo._column = attribuleValue;
			else
				propertyInfo._unColumn = true;

			return propertyInfo;
		}
	}
}
