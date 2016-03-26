using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Automao.Data.Mapping
{
	public class ProcedureNode
	{
		#region 字段
		private System.Type _entityType;
		private string _name;
		private string _procedure;
		private string _schema;
		private List<ProcedureParameterNode> _parameterList;
		#endregion

		#region 属性
		public string Name
		{
			get
			{
				return _name;
			}
		}

		public string Procedure
		{
			get
			{
				return _procedure;
			}
		}

		public string Schema
		{
			get
			{
				return _schema;
			}
		}

		public List<ProcedureParameterNode> ParameterList
		{
			get
			{
				return _parameterList;
			}
		}

		/// <summary>
		/// 所在文件路径
		/// </summary>
		public string MappingFileFullName
		{
			get;
			set;
		}
		#endregion

		#region 公共方法
		public string GetProcedureName(bool caseSensitive)
		{
			var schema = string.IsNullOrEmpty(_schema) ? "" : (_schema + ".");
			var procedureName = string.Format(caseSensitive ? "{0}\"{1}\"" : "{0}{1}", schema, _procedure);
			return procedureName;
		}
		#endregion

		#region 静态方法
		public static ProcedureNode Create(XElement element)
		{
			string attribuleValue;
			var info = new ProcedureNode();
			info._parameterList = new List<ProcedureParameterNode>();

			if(MappingInfo.GetAttribuleValue(element, "name", out attribuleValue))
				info._name = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "procedure", out attribuleValue))
				info._procedure = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "schema", out attribuleValue))
				info._schema = attribuleValue;

			foreach(var property in element.Elements())
			{
				var parameter = ProcedureParameterNode.Create(property);
				info.ParameterList.Add(parameter);
			}

			return info;
		}
		#endregion
	}
}
