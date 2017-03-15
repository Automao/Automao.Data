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

namespace Automao.Data.Mapping
{
	public class MappingInfo
	{
		#region 字段
		private List<ClassNode> _classNodeList;
		private List<ProcedureNode> _procedureNodeList;
		#endregion

		#region 属性
		public List<ClassNode> ClassNodeList
		{
			get
			{
				return _classNodeList;
			}
		}

		public List<ProcedureNode> ProcedureNodeList
		{
			get
			{
				return _procedureNodeList;
			}
		}
		#endregion

		#region 静态方法
		public static MappingInfo Create()
		{
			var result = new MappingInfo();
			result._classNodeList = new List<ClassNode>();
			result._procedureNodeList = new List<ProcedureNode>();

			var contexts = GetMappingContext(Zongsoft.ComponentModel.ApplicationContextBase.Current.ApplicationDirectory);

			Dictionary<string, string> dicJoin = new Dictionary<string, string>();
			XElement xml;
			foreach(var item in contexts)
			{
				xml = XElement.Parse(item.Value);
				foreach(var element in xml.Elements())
				{
					if(!element.Name.LocalName.Equals("procedure", StringComparison.OrdinalIgnoreCase))
					{
						var info = ClassNode.Create(element);
						info.MappingFileFullName = item.Key;

						var temp = result._classNodeList.FirstOrDefault(p => p.Name.Equals(info.Name, StringComparison.OrdinalIgnoreCase));
						if(temp != null)
							throw new Exception(string.Format("文件[{0}]和文件[{1}]中同时存在\"{2}\"节点", temp.MappingFileFullName, info.MappingFileFullName, info.Name));

						result._classNodeList.Add(info);
					}
					else
					{
						var info = ProcedureNode.Create(element);
						info.MappingFileFullName = item.Key;

						var temp = result._procedureNodeList.FirstOrDefault(p => p.Name.Equals(info.Name, StringComparison.OrdinalIgnoreCase));
						if(temp != null)
							throw new Exception(string.Format("文件[{0}]和文件[{1}]中同时存在\"{2}\"节点", temp.MappingFileFullName, info.MappingFileFullName, info.Name));

						result._procedureNodeList.Add(info);
					}
				}
			}

			result._classNodeList.ForEach(p =>
			{
				p.Init(result._classNodeList);

				p.JoinList.ForEach(pp =>
				{
					pp.Init(p, result._classNodeList);
				});
			});

			return result;
		}

		private static IDictionary<string, string> GetMappingContext(string rootDirectory)
		{
			if(string.IsNullOrWhiteSpace(rootDirectory))
				throw new ArgumentNullException(nameof(rootDirectory));

			var paths = Directory.GetFiles(rootDirectory, "*.mapping", SearchOption.AllDirectories);
			var result = new Dictionary<string, string>();

			foreach(var path in paths)
			{
				result.Add(path, File.ReadAllText(path));
			}

			return result;
		}

		[Obsolete]
		private static Dictionary<string, string> GetMappingContext(string[] paths, string mappingFileName)
		{
			var result = new Dictionary<string, string>();

			foreach(var path in paths)
			{
				if(string.IsNullOrWhiteSpace(path) || result.ContainsKey(path))
					continue;

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

		public static bool GetAttribuleValue(XElement element, string name, out string value)
		{
			var attribule = element.Attribute(name);
			if(attribule == null)
			{
				value = string.Empty;
				return false;
			}
			value = attribule.Value.Trim();

			if(string.IsNullOrWhiteSpace(value))
				return false;

			return true;
		}
		#endregion
	}
}
