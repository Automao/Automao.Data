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
		#region 静态字段
		private static readonly object _lock = new object();
		#endregion

		#region 字段
		private string _typeStr;
		private Type _type;
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
				return _typeStr;
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
				if(_type != null)
					return _type;

				if(!string.IsNullOrEmpty(_typeStr))
				{
					_type = System.Type.GetType(_typeStr);
					if(_type != null)
						return _type;
					throw new Exception(string.Format("当前节点({0})的Type出错", Name));
				}
				throw new Exception(string.Format("当前节点({0})的Type未设置", Name));
			}
		}
		#endregion

		#region 公共方法
		public string GetTableName()
		{
			if(string.IsNullOrWhiteSpace(_schema))
				return string.Format("`{0}`", _table);
			else
				return string.Format("`{0}`.`{1}`", _schema, _table);
		}

		///// <summary>
		///// 设置不同于配置的类型
		///// </summary>
		///// <param name="type"></param>
		//public void SetEntityType(Type type)
		//{
		//	if(!string.IsNullOrEmpty(_typeStr) && EntityType == type)
		//		return;

		//	foreach(var item in JoinList)
		//	{
		//		var propertyInfo = type.GetProperty(item.Name);
		//		if(propertyInfo != null)
		//		{
		//			if(string.IsNullOrEmpty(item.Target._typeStr) || item.Target.EntityType != propertyInfo.PropertyType)
		//				item.Target.SetEntityType(propertyInfo.PropertyType);
		//		}
		//	}
		//}

		public void Init(List<ClassNode> all)
		{
			if(!string.IsNullOrEmpty(Inherit))
			{
				_base = all.FirstOrDefault(p => p.Name.Equals(Inherit, StringComparison.OrdinalIgnoreCase));
				if(_base != null)
				{
					var join = new JoinPropertyNode(_base._name, _base, JoinType.Inner);
					var pks = _propertyNodeList.Where(p => p.IsKey).ToList();
					if(pks.Count == 0)
					{
						foreach(var pk in _base._propertyNodeList.Where(p => p.IsKey))
						{
							join.Member.Add(pk, pk);
						}
					}
					else
					{
						foreach(var pk in pks)
						{
							PropertyNode pn = _base._propertyNodeList.First(p => p.IsKey && p.Name.Equals(pk.Name, StringComparison.OrdinalIgnoreCase));
							if(pn == null)
								throw new Exception(string.Format("继承时子类主键名称要和父类主键名称一致：{0} {1}",this._name,_base._name));
							join.Member.Add(pk, pn);
						}
					}
					_joinList.Add(join);
				}
			}
		}

		public PropertyNode GetPropertyNode(string property)
		{
			var node = _propertyNodeList.FirstOrDefault(p => p.Name.Equals(property, StringComparison.OrdinalIgnoreCase));
			if(node != null)
				return node;

			var propertyInfo = this.EntityType.GetProperty(property, System.Reflection.BindingFlags.DeclaredOnly | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
			if(propertyInfo != null)
			{
				node = new PropertyNode(property);
				AddPropertyNodel(node);
				return node;
			}

			return null;
		}

		public void AddPropertyNodel(PropertyNode node)
		{
			if(_propertyNodeList.Any(p => p.Name.Equals(node.Name, StringComparison.OrdinalIgnoreCase)))
				return;

			lock(_lock)
			{
				if(_propertyNodeList.Any(p => p.Name.Equals(node.Name, StringComparison.OrdinalIgnoreCase)))
					return;

				var list = new List<PropertyNode>(_propertyNodeList);
				list.Add(node);
				_propertyNodeList = list;
			}
		}
		#endregion

		#region 静态方法
		public static ClassNode Create(XElement element)
		{
			string attribuleValue;

			var info = new ClassNode();

			if(MappingInfo.GetAttribuleValue(element, "type", out attribuleValue))
				info._typeStr = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "name", out attribuleValue) || !(attribuleValue = element.Name.LocalName).Equals("object", StringComparison.OrdinalIgnoreCase))
				info._name = attribuleValue;
			else
				info._name = info.EntityType.Name;

			if(MappingInfo.GetAttribuleValue(element, "table", out attribuleValue))
				info._table = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "schema", out attribuleValue))
				info._schema = attribuleValue;

			if(MappingInfo.GetAttribuleValue(element, "inherits", out attribuleValue))
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
						info.AddPropertyNodel(keyPropertyInfo);
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

				info.AddPropertyNodel(propertyInfo);
			}

			return info;
		}
		#endregion
	}
}
