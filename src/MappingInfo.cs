/*
 * Authors:
 *   喻星(Xing Yu) <491718907@qq.com>
 *
 * Copyright (C) 2015 Automao Network Co., Ltd. <http://www.zongsoft.com>
 *
 * This file is part of Automao.Data.
 *
 * Automao.Data is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * Automao.Data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Automao.Data; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Xml.Linq;

namespace Automao.Data
{
	public class MappingInfo
	{
		private List<ClassInfo> _mappingList;

		public MappingInfo(Dictionary<string, string> contexts)
		{
			Init(contexts);
		}

		private void Init(Dictionary<string, string> contexts)
		{
			_mappingList = new List<ClassInfo>();
			Dictionary<string, string> dicJoin = new Dictionary<string, string>();
			XElement xml;
			foreach(var item in contexts)
			{
				xml = XElement.Parse(item.Value);
				foreach(var element in xml.Elements())
				{
					ClassInfo info = new ClassInfo();
					info.MappingFileFullName = item.Key;
					info.ClassName = element.Name.ToString();

					var temp = _mappingList.FirstOrDefault(p => p.ClassName.Equals(info.ClassName, StringComparison.OrdinalIgnoreCase));
					if(temp != null)
						throw new Exception(string.Format("文件[{0}]和文件[{1}]中同时存在\"{2}\"节点", temp.MappingFileFullName, info.MappingFileFullName, info.ClassName));

					if(element.Attribute("Table") != null && !string.IsNullOrEmpty(element.Attribute("Table").Value))
						info.TableName = element.Attribute("Table").Value.Trim();
					else if(element.Attribute("Procedure") != null && !string.IsNullOrEmpty(element.Attribute("Procedure").Value))
					{
						info.TableName = element.Attribute("Procedure").Value.Trim();
						info.IsProcedure = true;
					}
					else
						info.TableName = info.ClassName;

					if(element.Attribute("Assembly") != null && !string.IsNullOrEmpty(element.Attribute("Assembly").Value))
						info.Assembly = element.Attribute("Assembly").Value;
					else
						info.Assembly = "System.Object,mscorlib";

					info.PropertyInfoList = new List<ClassPropertyInfo>();
					foreach(var property in element.Elements())
					{
						ClassPropertyInfo propertyInfo = new ClassPropertyInfo();
						propertyInfo.ClassPropertyName = property.Name.ToString();
						if(property.Attribute("ConstructorName") != null && !string.IsNullOrEmpty(property.Attribute("ConstructorName").Value))
						{
							propertyInfo.ConstructorName = property.Attribute("ConstructorName").Value;
							propertyInfo.PassedIntoConstructor = true;
						}

						if(property.Attribute("PKColumn") != null && !string.IsNullOrEmpty(property.Attribute("PKColumn").Value))
						{
							propertyInfo.TableColumnName = property.Attribute("PKColumn").Value;
							propertyInfo.IsPKColumn = true;
						}
						if(property.Attribute("OutPut") != null && !string.IsNullOrEmpty(property.Attribute("OutPut").Value.Trim()))
						{
							propertyInfo.TableColumnName = property.Attribute("OutPut").Value.Trim();
							propertyInfo.IsOutPutParamer = true;
						}

						if(property.Attribute("Column") != null)
							propertyInfo.TableColumnName = property.Attribute("Column").Value.Trim();

						if(property.Attribute("Join") != null && !string.IsNullOrEmpty(property.Attribute("Join").Value.Trim()))
						{
							propertyInfo.IsFKColumn = true;
							dicJoin.Add(string.Format("{0},{1}", info.ClassName, propertyInfo.ClassPropertyName), property.Attribute("Join").Value);

							if(property.Attribute("Set") != null && !string.IsNullOrEmpty(property.Attribute("Set").Value.Trim()))
								propertyInfo.SetClassPropertyName = property.Attribute("Set").Value.Trim();
							else
								throw new FormatException(string.Format("{0}.{1}节点Set属性未赋值", info.ClassName, propertyInfo.ClassPropertyName));
						}

						if(property.Attribute("Nullable") != null && !string.IsNullOrEmpty(property.Attribute("Nullable").Value.Trim()))
						{
							bool flag;
							propertyInfo.Nullable = bool.TryParse(property.Attribute("Nullable").Value, out flag) ? flag : false;
						}
						else
							propertyInfo.Nullable = false;

						if(property.Attribute("DbType") != null && !string.IsNullOrEmpty(property.Attribute("DbType").Value.Trim()))
							propertyInfo.DbType = property.Attribute("DbType").Value.Trim();
						if(property.Attribute("Size") != null && !string.IsNullOrEmpty(property.Attribute("Size").Value.Trim()))
						{
							int size;
							propertyInfo.Size = int.TryParse(property.Attribute("Size").Value.Trim(), out size) ? (int?)size : null;
						}

						if(string.IsNullOrEmpty(propertyInfo.TableColumnName))
							propertyInfo.TableColumnName = propertyInfo.ClassPropertyName;

						propertyInfo.Host = info;
						info.PropertyInfoList.Add(propertyInfo);
					}
					_mappingList.Add(info);
				}
			}
			_mappingList.ForEach(p =>
			{
				p.PropertyInfoList.ForEach(pp =>
				{
					if(pp.IsFKColumn)
					{
						var joinClassName = dicJoin[string.Format("{0},{1}", p.ClassName, pp.ClassPropertyName)];
						var array = joinClassName.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
						var classInfo = _mappingList.FirstOrDefault(c => c.ClassName.Equals(array[0], StringComparison.OrdinalIgnoreCase));
						if(classInfo != null)
						{
							pp.Join = classInfo;
							pp.JoinColumn = classInfo.PropertyInfoList.FirstOrDefault(pi => array.Length > 1 ? pi.TableColumnName.Equals(array[1], StringComparison.OrdinalIgnoreCase) : pi.IsPKColumn);
						}
					}
				});
			});
		}

		/// <summary>
		/// 对应关系集合
		/// </summary>
		public List<ClassInfo> MappingList
		{
			get
			{
				return _mappingList;
			}
		}
	}
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

	/// <summary>
	/// 属性详情
	/// </summary>
	public class ClassPropertyInfo
	{
		public ClassInfo Host
		{
			get;
			set;
		}

		/// <summary>
		/// 类中属性名称
		/// </summary>
		public string ClassPropertyName
		{
			get;
			set;
		}
		/// <summary>
		/// 表中列名
		/// </summary>
		public string TableColumnName
		{
			get;
			set;
		}
		/// <summary>
		/// 数据类型
		/// </summary>
		public string DbType
		{
			get;
			set;
		}
		/// <summary>
		/// 数据大小
		/// </summary>
		public int? Size
		{
			get;
			set;
		}
		/// <summary>
		/// 是否为主键
		/// </summary>
		public bool IsPKColumn
		{
			get;
			set;
		}

		/// <summary>
		/// 是否传入构造函数
		/// </summary>
		public bool PassedIntoConstructor
		{
			get;
			set;
		}
		/// <summary>
		/// 构造函数对应参数名称
		/// </summary>
		public string ConstructorName
		{
			get;
			set;
		}
		/// <summary>
		/// 是否为输出参数
		/// </summary>
		public bool IsOutPutParamer
		{
			get;
			set;
		}
		/// <summary>
		/// 是否为外键
		/// </summary>
		public bool IsFKColumn
		{
			get;
			set;
		}
		/// <summary>
		/// 是否可为空
		/// </summary>
		public bool Nullable
		{
			get;
			set;
		}
		/// <summary>
		/// 外键关联主表
		/// </summary>
		public ClassInfo Join
		{
			get;
			set;
		}
		/// <summary>
		/// 外键关联列
		/// </summary>
		public ClassPropertyInfo JoinColumn
		{
			get;
			set;
		}
		/// <summary>
		/// 要将关联对像赋值给当前名称指定的属性
		/// </summary>
		public string SetClassPropertyName
		{
			get;
			set;
		}
	}
}
