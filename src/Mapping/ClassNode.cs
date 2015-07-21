using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Automao.Data.Mapping
{

	/// <summary>
	/// 类详情
	/// </summary>
	public class ClassNode
	{
		#region 字段
		private Type _entityType;
		private string _type;
		private string _name;
		private string _schema;
		private string _table;
		private List<PropertyNode> _propertyNodeList;
		private List<JoinPropertyNode> _joinList;
		private string _inherit;
		private ClassNode _base;
		#endregion

		#region 属性
		/// <summary>
		/// 类信息
		/// </summary>
		public string Type
		{
			get
			{
				return _type;
			}
		}

		/// <summary>
		/// 类名
		/// </summary>
		public string Name
		{
			get
			{
				return _name;
			}
		}

		/// <summary>
		/// 表的所有者
		/// </summary>
		public string Schema
		{
			get
			{
				return _schema;
			}
		}

		/// <summary>
		/// 表名
		/// </summary>
		public string Table
		{
			get
			{
				return _table;
			}
		}

		/// <summary>
		/// 属性集合
		/// </summary>
		public List<PropertyNode> PropertyNodeList
		{
			get
			{
				return _propertyNodeList;
			}
		}

		/// <summary>
		/// JoinInfoList
		/// </summary>
		public List<JoinPropertyNode> JoinList
		{
			get
			{
				return _joinList;
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

		/// <summary>
		/// 继承
		/// </summary>
		public string Inherit
		{
			get
			{
				return _inherit;
			}
		}

		public ClassNode BaseClassNode
		{
			get
			{
				return _base;
			}
		}

		public Type EntityType
		{
			get
			{
				if(_entityType == null && !string.IsNullOrEmpty(Type))
				{
					_entityType = System.Type.GetType(Type);
					if(_entityType == null)
						throw new Exception(string.Format("当前节点({0})的Type出错", Name));
				}

				return _entityType;
			}
		}
		#endregion

		#region 公共方法
		public string GetTableName(bool caseSensitive)
		{
			var schema = string.IsNullOrEmpty(_schema) ? "" : (_schema + ".");
			var tableName = string.Format(caseSensitive ? "{0}\"{1}\"" : "{0}{1}", schema, _table);
			return tableName;
		}

		public void SetEntityType(Type type)
		{
			if(_entityType == type)
				return;

			_entityType = type;

			foreach(var item in JoinList)
			{
				var propertyInfo = _entityType.GetProperty(item.Name);
				if(propertyInfo != null)
					item.Target.SetEntityType(propertyInfo.PropertyType);
			}
		}

		public void Init(List<ClassNode> all)
		{
			if(!string.IsNullOrEmpty(Inherit))
			{
				_base = all.FirstOrDefault(p => p.Name.Equals(Inherit, StringComparison.OrdinalIgnoreCase));
				if(_base != null)
				{
					var join = new JoinPropertyNode(_base._name, _base, JoinType.Inner);
					var pks = _propertyNodeList.Where(p => p.IsKey);
					foreach(var pk in pks)
					{
						join.Member.Add(pk, _base._propertyNodeList.First(p => p.IsKey && p.Name.Equals(pk.Name, StringComparison.OrdinalIgnoreCase)));
					}
					_joinList.Add(join);
				}
			}
		}
		#endregion

		#region 静态方法
		public static ClassNode Create(XElement element)
		{
			string attribuleValue;

			var info = new ClassNode();

			if(MappingInfo.GetAttribuleValue(element, "type", out attribuleValue))
				info._type = attribuleValue;
			else
				info._type = "System.Object,mscorlib";

			if(MappingInfo.GetAttribuleValue(element, "name", out attribuleValue) || !(attribuleValue = element.Name.LocalName).Equals("object", StringComparison.OrdinalIgnoreCase))
				info._name = attribuleValue;
			else
				info._name = info.EntityType.Name;

			if(MappingInfo.GetAttribuleValue(element, "table", out attribuleValue))
				info._table = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "schema", out attribuleValue))
				info._schema = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "Inherit", out attribuleValue))
				info._inherit = attribuleValue;

			info._propertyNodeList = new List<PropertyNode>();
			info._joinList = new List<JoinPropertyNode>();

			foreach(var property in element.Elements())
			{
				if(property.Name.LocalName.Equals("key", StringComparison.OrdinalIgnoreCase))
				{
					foreach(var key in property.Elements())
					{

						var keyPropertyInfo = PropertyNode.Create(key);
						if(string.IsNullOrWhiteSpace(keyPropertyInfo.Name))
							throw new FormatException(string.Format("{0}节点下面有一个主键没有name属性", info.Name));
						keyPropertyInfo.IsKey = true;
						info.PropertyNodeList.Add(keyPropertyInfo);
					}
					continue;
				}
				else if(property.HasElements)
				{
					var joinPropertyInfo = JoinPropertyNode.Create(info, property);
					info.JoinList.Add(joinPropertyInfo);
					continue;
				}

				var propertyInfo = PropertyNode.Create(property);
				if(string.IsNullOrWhiteSpace(propertyInfo.Name))
					throw new FormatException(string.Format("{0}节点下面有一个property节点没有name属性", info.Name));

				info.PropertyNodeList.Add(propertyInfo);
			}

			return info;
		}
		#endregion
	}
}
