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
		private string _field;
		private bool _isKey;
		private bool _ignored;
		private bool _passedIntoConstructor;
		private string _parameterName;
		private bool _sequenced;
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
		/// 忽略映射
		/// </summary>
		public bool Ignored
		{
			get
			{
				return _ignored;
			}
		}

		/// <summary>
		/// 表中列名
		/// </summary>
		public string Field
		{
			get
			{
				if(string.IsNullOrEmpty(_field))
					_field = Name;

				return _field;
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
		public string ParameterName
		{
			get
			{
				return _parameterName;
			}
		}

		public bool Sequenced
		{
			get
			{
				return _sequenced;
			}
		}
		#endregion

		public static PropertyNode Create(XElement property)
		{
			string propertyInfoName = null, attribuleValue;

			if(MappingInfo.GetAttribuleValue(property, "name", out attribuleValue) || !(attribuleValue = property.Name.LocalName).Equals("property", StringComparison.OrdinalIgnoreCase))
				propertyInfoName = attribuleValue;

			var propertyInfo = new PropertyNode(propertyInfoName);

			if(MappingInfo.GetAttribuleValue(property, "constructor.parameter", out attribuleValue))
			{
				propertyInfo._parameterName = attribuleValue;
				propertyInfo._passedIntoConstructor = true;
			}

			if(property.Attribute("field") == null)
				propertyInfo._field = propertyInfo.Name;
			else if(MappingInfo.GetAttribuleValue(property, "field", out attribuleValue))
				propertyInfo._field = attribuleValue;
			else
				propertyInfo._ignored = true;

			if(MappingInfo.GetAttribuleValue(property, "sequenced", out attribuleValue))
				propertyInfo._sequenced = true;

			return propertyInfo;
		}
	}
}
