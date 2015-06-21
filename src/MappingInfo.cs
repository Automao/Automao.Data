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
		public static List<ClassInfo> CreateClassInfo(string[] paths, string mappingFileName)
		{
			if(paths == null || paths.Length == 0)
				paths = new[] { AppDomain.CurrentDomain.BaseDirectory };
			else
				paths = paths.Select(p => ParsePath(p)).ToArray();

			var contexts = GetMappingContext(paths, mappingFileName);

			var result = new List<ClassInfo>();
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

					var temp = result.FirstOrDefault(p => p.ClassName.Equals(info.ClassName, StringComparison.OrdinalIgnoreCase));
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
					result.Add(info);
				}
			}

			result.ForEach(p =>
			{
				p.PropertyInfoList.ForEach(pp =>
				{
					if(pp.IsFKColumn)
					{
						var joinClassName = dicJoin[string.Format("{0},{1}", p.ClassName, pp.ClassPropertyName)];
						var array = joinClassName.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
						var classInfo = result.FirstOrDefault(c => c.ClassName.Equals(array[0], StringComparison.OrdinalIgnoreCase));
						if(classInfo != null)
						{
							pp.Join = classInfo;
							pp.JoinColumn = classInfo.PropertyInfoList.FirstOrDefault(pi => array.Length > 1 ? pi.TableColumnName.Equals(array[1], StringComparison.OrdinalIgnoreCase) : pi.IsPKColumn);
						}
					}
				});
			});

			return result;
		}

		private static Dictionary<string, string> GetMappingContext(string[] paths, string mappingFileName)
		{
			Console.WriteLine("[{0}]", string.Join("\r\n", paths));

			var result = new Dictionary<string, string>();
			foreach(var path in paths)
			{
				if(result.ContainsKey(path))
					continue;

				if(path.StartsWith("http://"))
				{
					var wc = new System.Net.WebClient();
					var buffer = wc.DownloadData(path);
					var text = Encoding.UTF8.GetString(buffer);
					result.Add(path, text);
				}
				else
				{
					var di = new DirectoryInfo(path);
					if(di.Exists)
					{
						var files = System.IO.Directory.GetFiles(path, mappingFileName + ".mapping", System.IO.SearchOption.AllDirectories);

						var temp = GetMappingContext(files, mappingFileName);
						foreach(var item in temp)
						{
							result.Add(item.Key, item.Value);
						}
					}
					else
					{
						var fi = new FileInfo(path);
						if(fi.Exists)
						{
							using(var sr = fi.OpenText())
							{
								var text = sr.ReadToEnd();
								result.Add(path, text);
							}
						}
					}
				}
			}
			return result;
		}

		private static string ParsePath(string path)
		{
			if(!path.Contains("..\\"))
				return path;

			var index = path.IndexOf("..\\");
			var directory = path.Substring(0, index);
			if(string.IsNullOrEmpty(directory))
				directory = Path.GetDirectoryName(typeof(MappingInfo).Assembly.Location);
			directory = Directory.GetParent(directory).FullName;

			path = Path.Combine(directory, path.Substring(index + 3));
			return ParsePath(path);
		}
	}
}
