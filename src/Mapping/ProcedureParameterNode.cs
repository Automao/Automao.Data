using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Automao.Data.Mapping
{
	public class ProcedureParameterNode
	{
		#region 字段
		private string _name;
		private string _dbType;
		private int? _size;
		private bool _isOutPut;
		#endregion

		#region 属性
		/// <summary>
		/// 名称
		/// </summary>
		public string Name
		{
			get
			{
				return _name;
			}
		}

		/// <summary>
		/// 数据类型
		/// </summary>
		public string DbType
		{
			get
			{
				return _dbType;
			}
		}

		/// <summary>
		/// 数据大小
		/// </summary>
		public int? Size
		{
			get
			{
				return _size;
			}
		}

		/// <summary>
		/// 是否为输出参数
		/// </summary>
		public bool IsOutPut
		{
			get
			{
				return _isOutPut;
			}
		}
		#endregion

		#region 静态方法
		public static ProcedureParameterNode Create(XElement property)
		{
			var attribuleValue = string.Empty;
			var parameter = new ProcedureParameterNode();

			if(MappingInfo.GetAttribuleValue(property, "name", out attribuleValue) || !(attribuleValue = property.Name.LocalName).Equals("parameter"))
				parameter._name = attribuleValue;

			if(MappingInfo.GetAttribuleValue(property, "output", out attribuleValue))
			{
				bool isoutput;
				parameter._isOutPut = bool.TryParse(attribuleValue, out isoutput) ? isoutput : false;
			}

			if(MappingInfo.GetAttribuleValue(property, "dbType", out attribuleValue))
				parameter._dbType = attribuleValue;

			if(MappingInfo.GetAttribuleValue(property, "size", out attribuleValue))
			{
				int size;
				parameter._size = int.TryParse(attribuleValue, out size) ? (int?)size : null;
			}

			return parameter;
		}
		#endregion
	}
}
