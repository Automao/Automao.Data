using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Automao.Data.Mapping
{
	public class JoinPropertyNode
	{
		#region 字段
		private string _name;
		private string _targetStr;
		private ClassNode _target;
		private Dictionary<string, string> _temp;
		private Dictionary<PropertyNode, PropertyNode> _member;
		private JoinType _type;
		#endregion

		#region 构造函数
		public JoinPropertyNode(string name, string target, JoinType type = JoinType.Inner)
		{
			_name = name;
			_targetStr = target;
			_type = type;
			_temp = new Dictionary<string, string>();
		}

		public JoinPropertyNode(string name, ClassNode target, JoinType type = JoinType.Inner)
		{
			_name = name;
			_target = target;
			_type = type;
			_member = new Dictionary<PropertyNode, PropertyNode>();
		}
		#endregion

		#region 属性
		public string Name
		{
			get
			{
				return _name;
			}
		}

		public ClassNode Target
		{
			get
			{
				return _target;
			}
		}

		public JoinType Type
		{
			get
			{
				return _type;
			}
		}

		/// <summary>
		/// 关系
		/// </summary>
		public Dictionary<PropertyNode, PropertyNode> Member
		{
			get
			{
				return _member;
			}
		}
		#endregion

		#region 方法
		public void Add(string from, string to)
		{
			_temp.Add(from, to);
		}

		public void Init(ClassNode host, List<ClassNode> all)
		{
			_target = all.FirstOrDefault(p => p.Name.Equals(_targetStr, StringComparison.OrdinalIgnoreCase));

			_member = new Dictionary<PropertyNode, PropertyNode>();
			foreach(var key in _temp.Keys)
			{
				var fromPropertyInfo = host.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(key, StringComparison.OrdinalIgnoreCase));
				if(fromPropertyInfo == null)
				{
					fromPropertyInfo = new PropertyNode(key);
					host.PropertyNodeList.Add(fromPropertyInfo);
				}

				var value = _temp[key];
				var toPropertyInfo = _target.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(value, StringComparison.OrdinalIgnoreCase));
				if(toPropertyInfo == null)
				{
					toPropertyInfo = new PropertyNode(value);
					_target.PropertyNodeList.Add(toPropertyInfo);
				}

				_member.Add(fromPropertyInfo, toPropertyInfo);
			}
		}
		#endregion

		public static JoinPropertyNode Create(ClassNode info, XElement property)
		{
			string name = null, target, attribuleValue;

			if(MappingInfo.GetAttribuleValue(property, "name", out attribuleValue))
				name = attribuleValue;
			else
				throw new FormatException(string.Format("{0}.property节点name属性未赋值", info.Name));

			if(MappingInfo.GetAttribuleValue(property, "relationTo", out attribuleValue))
				target = attribuleValue;
			else
				throw new FormatException(string.Format("{0}.property节点relationTo属性未赋值", info.Name));

			var joinPropertyInfo = new JoinPropertyNode(name, target);

			var join = property.Element("join");

			if(MappingInfo.GetAttribuleValue(join, "mode", out attribuleValue))
			{
				JoinType temp;
				joinPropertyInfo._type = Zongsoft.Common.Convert.TryConvertValue<JoinType>(attribuleValue, out temp) ? temp : JoinType.Inner;
			}

			foreach(var relation in join.Elements())
			{
				string from, to;
				if(MappingInfo.GetAttribuleValue(relation, "from", out attribuleValue))
					from = attribuleValue;
				else
					throw new FormatException(string.Format("{0}.property节点中有member项from属性未赋值", info.Name));

				if(MappingInfo.GetAttribuleValue(relation, "to", out attribuleValue))
					to = attribuleValue;
				else
					throw new FormatException(string.Format("{0}.property节点中有member项to属性未赋值", info.Name));

				joinPropertyInfo.Add(from, to);
			}

			return joinPropertyInfo;
		}
	}
}
