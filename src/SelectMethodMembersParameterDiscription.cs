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

namespace Automao.Data
{
	/// <summary>
	/// 查询方法Members参数的描述
	/// </summary>
	internal class SelectMethodMembersParameterDiscription
	{
		#region 字段
		private Dictionary<string, Tuple<string, Info, ClassPropertyInfo>> _selectColumns;
		private Dictionary<string, Tuple<string, Info, ClassPropertyInfo>> _otherColumns;
		private Dictionary<string, ClassInfo> _classInfoMappings;
		private Dictionary<string, Info> _navigationInfo;
		#endregion

		#region 构造函数
		public SelectMethodMembersParameterDiscription(ClassInfo root, IEnumerable<string> members, IEnumerable<string> other)
		{
			_otherColumns = new Dictionary<string, Tuple<string, Info, ClassPropertyInfo>>();
			_selectColumns = new Dictionary<string, Tuple<string, Info, ClassPropertyInfo>>();
			_classInfoMappings = new Dictionary<string, ClassInfo>();
			_navigationInfo = new Dictionary<string, Info>();

			RootInfo = new Info();
			RootInfo.ClassInfo = root;
			RootInfo.TableEx = "T";
			RootInfo.Propertys = new List<string>();
			RootInfo.NavigationPropertyInfos = new Dictionary<string, Info>();

			_classInfoMappings.Add(RootInfo.TableEx, root);

			foreach(var member in members)
			{
				if(member.IndexOf('.') <= 0)
				{
					var value = new Tuple<string, Info, ClassPropertyInfo>(member, RootInfo, root.PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName.Equals(member, StringComparison.OrdinalIgnoreCase)));
					_selectColumns.Add(member, value);
					RootInfo.Propertys.Add(member);
				}
				else
				{
					var propertyAndInfo = CreateInfoAndAddToCache(member, 0, RootInfo, _navigationInfo);
					if(propertyAndInfo != null)
					{
						_selectColumns.Add(member, propertyAndInfo);
						if(!_classInfoMappings.ContainsKey(propertyAndInfo.Item2.TableEx))
							_classInfoMappings.Add(propertyAndInfo.Item2.TableEx, propertyAndInfo.Item2.ClassInfo);
					}
				}
			}

			if(other != null)
			{
				foreach(var item in other)
				{
					if(_otherColumns.ContainsKey(item))
						continue;

					if(item.IndexOf('.') <= 0)
					{
						_otherColumns.Add(item, new Tuple<string, Info, ClassPropertyInfo>(item, RootInfo, root.PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName.Equals(item, StringComparison.OrdinalIgnoreCase))));
						RootInfo.Propertys.Add(item);
					}
					else
					{
						var propertyAndInfo = CreateInfoAndAddToCache(item, 0, RootInfo, _navigationInfo);
						if(propertyAndInfo != null)
							_otherColumns.Add(item, propertyAndInfo);
					}
				}
			}
		}
		#endregion

		#region 属性
		public Info RootInfo
		{
			get;
			set;
		}

		public Dictionary<string, Tuple<string, Info, ClassPropertyInfo>> SelectColumns
		{
			get
			{
				return _selectColumns;
			}
		}

		/// <summary>
		/// Key:带导航属性的普通属性，Value:Item1：普通属性，Item2:Info，Item3:ClassPropertyInfo
		/// </summary>
		public Dictionary<string, Tuple<string, Info, ClassPropertyInfo>> OtherColumns
		{
			get
			{
				return _otherColumns;
			}
		}

		/// <summary>
		/// 别名和ClassInfo的映射
		/// </summary>
		public Dictionary<string, ClassInfo> ClassInfoMappings
		{
			get
			{
				return _classInfoMappings;
			}
		}

		public Dictionary<string, Info> NavigationInfo
		{
			get
			{
				return _navigationInfo;
			}
		}
		#endregion

		#region 方法
		private Tuple<string, Info, ClassPropertyInfo> CreateInfoAndAddToCache(string member, int startIndex, Info parentInfo, Dictionary<string, Info> cache)
		{
			var list = new List<Info>();
			var index = member.IndexOf('.', startIndex);

			var substr = member.Substring(0, index);
			Info info;
			if(cache.ContainsKey(substr))
				info = cache[substr];
			else
			{
				info = new Info();
				info.NavigationPropertyInfos = new Dictionary<string, Info>();
				var navigationProperty = substr.Substring(startIndex);
				parentInfo.NavigationPropertyInfos.Add(navigationProperty, info);
				info.ParentInfo = parentInfo;
				info.ParentJoinPropertyInfos = parentInfo.ClassInfo.PropertyInfoList.Where(p => p.IsFKColumn && p.SetClassPropertyName.Equals(navigationProperty, StringComparison.OrdinalIgnoreCase)).ToArray();
				if(info.ParentJoinPropertyInfos == null || info.ParentJoinPropertyInfos.Length == 0)
					return null;

				info.ClassInfo = info.ParentJoinPropertyInfos[0].Join;
				info.TableEx = "J" + cache.Count;
				info.Propertys = new List<string>();
				info.NavigationPropertys = substr;
				cache.Add(substr, info);
			}

			var tempIndex = member.IndexOf('.', index + 1);
			if(tempIndex < 0)
			{
				var property = member.Substring(index + 1);
				info.Propertys.Add(property);
				return new Tuple<string, Info, ClassPropertyInfo>(property, info, info.ClassInfo.PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName.Equals(property, StringComparison.OrdinalIgnoreCase)));
			}
			else
				return CreateInfoAndAddToCache(member, index + 1, info, cache);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="caseSensitive">区分大小写</param>
		/// <returns></returns>
		public string GetJoinSql(bool caseSensitive)
		{
			var joinformat = caseSensitive ? "{0} JOIN \"{1}\" {2} ON {3}" : "{0} JOIN {1} {2} ON {3}";
			var onformat = caseSensitive ? "{0}.\"{1}\"={2}.\"{3}\"" : "{0}.{1}={2}.{3}";

			return string.Join(" ", _navigationInfo.Values.Where(info => info.ParentInfo != null).Select(info =>
					string.Format(joinformat, info.ParentJoinPropertyInfos.FirstOrDefault().Nullable ? "LEFT" : "INNER", info.ClassInfo.TableName, info.TableEx,
					string.Join(" AND ", info.ParentJoinPropertyInfos
					.Select(p => string.Format(onformat, info.ParentInfo.TableEx, p.TableColumnName, info.TableEx, p.JoinColumn.TableColumnName))))));
		}
		#endregion

		#region 内部类
		/// <summary>
		/// 导航属性的详情
		/// </summary>
		internal class Info
		{
			/// <summary>
			/// 表别名
			/// </summary>
			public string TableEx
			{
				get;
				set;
			}

			/// <summary>
			/// <paramref name="Propertys"/>所在类对应的ClassInfo
			/// </summary>
			public ClassInfo ClassInfo
			{
				get;
				set;
			}

			/// <summary>
			/// 纯导航属性
			/// </summary>
			public string NavigationPropertys
			{
				get;
				set;
			}

			/// <summary>
			/// 不包含导航属性的普通属性集合
			/// </summary>
			public List<string> Propertys
			{
				get;
				set;
			}

			public Info ParentInfo
			{
				get;
				set;
			}

			public Dictionary<string, Info> NavigationPropertyInfos
			{
				get;
				set;
			}

			public ClassPropertyInfo[] ParentJoinPropertyInfos
			{
				get;
				set;
			}
		}
		#endregion
	}
}
