using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automao.Data
{

	/// <summary>
	/// 类详情
	/// </summary>
	public class ClassInfo
	{
		private Type _entityType;
		/// <summary>
		/// 类信息
		/// </summary>
		public string Assembly
		{
			get;
			set;
		}
		/// <summary>
		/// 类名
		/// </summary>
		public string ClassName
		{
			get;
			set;
		}
		/// <summary>
		/// 表名
		/// </summary>
		public string TableName
		{
			get;
			set;
		}
		/// <summary>
		/// 是否是存储过程
		/// </summary>
		public bool IsProcedure
		{
			get;
			set;
		}
		/// <summary>
		/// 属性集合
		/// </summary>
		public List<ClassPropertyInfo> PropertyInfoList
		{
			get;
			set;
		}

		/// <summary>
		/// 所在文件路径
		/// </summary>
		public string MappingFileFullName
		{
			get;
			set;
		}

		public Type EntityType
		{
			get
			{
				if(_entityType == null)
					_entityType = GetEntityType();
				return _entityType;
			}
		}

		public string GetColumn(string name)
		{
			var array = name.Split(',').ToList();
			if(array.Count > 1)
			{
				var propertyInfo = PropertyInfoList.FirstOrDefault(p => p.IsFKColumn && p.SetClassPropertyName.Equals(array[0], StringComparison.OrdinalIgnoreCase));
				if(propertyInfo != null)
				{
					array.RemoveAt(0);
					return propertyInfo.Join.GetColumn(string.Join(".", array));
				}
			}

			var pi = PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(pi != null)
				return pi.TableColumnName;
			return name;
		}

		private Type GetEntityType()
		{
			if(string.IsNullOrEmpty(Assembly))
				throw new Exception(string.Format("当前节点({0})的Assembly为空", ClassName));
			var entityType = Type.GetType(Assembly);
			if(entityType == null)
				throw new Exception(string.Format("当前节点({0})的Assembly出错", ClassName));
			return entityType;
		}
	}
}
