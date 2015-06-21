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
using System.Text;

using Zongsoft.Data;

namespace Automao.Data
{
	public static class ObjectAccessExtension
	{
		internal static string FormatWhere(this string format, ICondition clause, Dictionary<string, ColumnInfo> columnInfos, bool caseSensitive, ref int startFormatIndex, out object[] values)
		{
			values = new object[0];
			if(clause == null)
				return "";

			var where = clause.Resolve(columnInfos, caseSensitive, ref startFormatIndex, out values);

			if(string.IsNullOrEmpty(where))
				return "";

			return string.Format(format, where);
		}

		/// <summary>
		/// 格式化sql的where部分
		/// </summary>
		/// <param name="format">格式化字符串:WHERE {0}</param>
		/// <param name="body"></param>
		/// <returns></returns>
		internal static string FormatWhere(this string format, ICondition clause, Func<string, Tuple<ClassInfo, string>> getTableEx, bool caseSensitive, ref int startFormatIndex, out object[] values)
		{
			values = new object[0];
			if(clause == null)
				return "";

			var where = clause.Resolve(getTableEx, caseSensitive, ref startFormatIndex, out values);

			if(string.IsNullOrEmpty(where))
				return "";

			return string.Format(format, where);
		}

		internal static string Resolve(this ICondition condition, Dictionary<string, ColumnInfo> columnInfos, bool caseSensitive, ref int startFormatIndex, out object[] values)
		{
			if(condition is Condition)
			{
				var where = (Condition)condition;
				if(where.Value == null)
					values = new object[0];
				else if(where.Value is object[])
					values = (object[])where.Value;
				else if(where.Value is Array)
				{
					var array = (Array)where.Value;
					values = new object[array.Length];
					array.CopyTo(values, 0);
				}
				else
					values = new[] { where.Value };

				if(!columnInfos.ContainsKey(where.Name))
					throw new Exception(string.Format("未找到属性\"{0}\"的描述信息", where.Name));

				var columnInfo = columnInfos[where.Name];
				var pi = columnInfo.PropertyInfo;

				var oper = where.Operator.Parse(values, ref startFormatIndex);

				var columnName = pi == null ? columnInfo.Field : pi.TableColumnName;

				if(string.IsNullOrEmpty(oper))
					return string.Format("{0} != {0}", columnInfo.ToColumn(caseSensitive));
				else
					return string.Format("{0} {1}", columnInfo.ToColumn(caseSensitive), oper);
			}
			else if(condition is ConditionCollection)
			{
				var where = (ConditionCollection)condition;
				var sqls = new List<string>();
				var vs = new List<object>();

				foreach(var item in where)
				{
					var result = item.Resolve(columnInfos, caseSensitive, ref startFormatIndex, out values);
					if(string.IsNullOrEmpty(result))
						continue;

					if(item is ConditionCollection)
						result = string.Format("({0})", result);

					sqls.Add(result);
					vs.AddRange(values);
				}
				values = vs.ToArray();

				if(sqls.Count == 0)
					return null;
				if(sqls.Count == 1)
					return sqls[0];

				return string.Format("{0}", string.Join(where.ConditionCombine.Parse(), sqls));
			}

			values = new object[0];
			return null;
		}

		/// <summary>
		/// 解析ICondition成sql里where部分
		/// </summary>
		/// <param name="clause"></param>
		/// <param name="startFormatIndex"></param>
		/// <param name="values"></param>
		/// <returns>例：T."Name"={0}</returns>
		public static string Resolve(this ICondition clause, Func<string, Tuple<ClassInfo, string>> getTableEx, bool caseSensitive, ref int startFormatIndex, out object[] values)
		{
			if(clause is Condition)
			{
				var where = (Condition)clause;
				if(where.Value == null)
					values = new object[0];
				else if(where.Value is object[])
					values = (object[])where.Value;
				else
					values = new[] { where.Value };

				var tuple = getTableEx(where.Name);

				var index = where.Name.LastIndexOf('.');
				var property = index < 0 ? where.Name : where.Name.Substring(index + 1);

				var pi = tuple.Item1.PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName.Equals(property, StringComparison.OrdinalIgnoreCase));

				var oper = where.Operator.Parse(values, ref startFormatIndex);

				var columnName = pi == null ? where.Name : pi.TableColumnName;

				string format;
				if(string.IsNullOrEmpty(oper))
				{
					if(string.IsNullOrEmpty(tuple.Item2))
						format = caseSensitive ? "{0}\"{1}\" !={0}\"{1}\"" : "{0}{1} !={0}{1}";
					else
						format = caseSensitive ? "{0}.\"{1}\" !={0}.\"{1}\"" : "{0}.{1} !={0}.{1}";
					return string.Format(format, tuple.Item2, columnName);
				}

				if(string.IsNullOrEmpty(tuple.Item2))
					format = caseSensitive ? "{0}\"{1}\" {2}" : "{0}{1} {2}";
				else
					format = caseSensitive ? "{0}.\"{1}\" {2}" : "{0}.{1} {2}";
				return string.Format(format, tuple.Item2, columnName, oper);
			}
			else if(clause is ConditionCollection)
			{
				var where = (ConditionCollection)clause;
				var sqls = new List<string>();
				var vs = new List<object>();

				foreach(var item in where)
				{
					var result = item.Resolve(getTableEx, caseSensitive, ref startFormatIndex, out values);
					if(string.IsNullOrEmpty(result))
						continue;

					if(item is ConditionCollection)
						result = string.Format("({0})", result);

					sqls.Add(result);
					vs.AddRange(values);
				}
				values = vs.ToArray();

				if(sqls.Count == 0)
					return null;
				if(sqls.Count == 1)
					return sqls[0];

				return string.Format("{0}", string.Join(where.ConditionCombine.Parse(), sqls));
			}

			values = new object[0];
			return null;
		}

		private static string Parse(this ConditionOperator clauseOperator, object[] values, ref int formatIndex)
		{
			switch(clauseOperator)
			{
				case ConditionOperator.Between:
					return string.Format("BETWEEN {{{0}}} AND {{{1}}}", formatIndex++, formatIndex++);
				case ConditionOperator.Equal:
				case ConditionOperator.Like:
					{
						if(values == null || values.Length == 0)
							return "IS NULL";
						return string.Format("{0} {{{1}}}", values[0] is string && ((string)values[0]).IndexOfAny("_%".ToArray()) >= 0 ? "LIKE" : "=", formatIndex++);
					}
				case ConditionOperator.GreaterThan:
					return string.Format("> {{{0}}}", formatIndex++);
				case ConditionOperator.GreaterThanEqual:
					return string.Format(">= {{{0}}}", formatIndex++);
				case ConditionOperator.In:
				case ConditionOperator.NotIn:
					{
						if(values == null || values.Length == 0)
							return "";

						if(values.Length == 1)
							return string.Format("{0} {{{1}}}", clauseOperator == ConditionOperator.NotIn ? "!=" : "=", formatIndex++);

						var list = new List<string>();
						for(int i = 0; i < values.Length; i++)
						{
							list.Add(string.Format("{{{0}}}", formatIndex + i));
						}

						formatIndex += values.Length;
						return string.Format("{0} ({1})", clauseOperator == ConditionOperator.NotIn ? "NOT IN" : "IN", string.Join(",", list));
					}

				case ConditionOperator.LessThan:
					return string.Format("< {{{0}}}", formatIndex++);
				case ConditionOperator.LessThanEqual:
					return string.Format("<= {{{0}}}", formatIndex++);
				case ConditionOperator.NotEqual:
					{
						if(values == null || values.Length == 0)
							return "IS NOT NULL";

						return string.Format("!= {{{0}}}", formatIndex++);
					}
				default:
					throw new ArgumentOutOfRangeException("未知的clauseOperator");
			}
		}

		private static string Parse(this ConditionCombine combine)
		{
			switch(combine)
			{
				case ConditionCombine.Or:
					return " OR ";
				default:
					return " AND ";
			}
		}

		private static string Parse(this SortingMode sortingMode)
		{
			switch(sortingMode)
			{
				case SortingMode.Ascending:
					return "ASC";
				case SortingMode.Descending:
					return "DESC";
				default:
					throw new ArgumentOutOfRangeException(string.Format("未存在当前枚举值:{0}", sortingMode));
			}
		}

		public static string Parse(this Sorting sorting, ClassInfo info, string tableEx)
		{
			if(!string.IsNullOrEmpty(tableEx))
				tableEx += ".";

			var sort = sorting.Mode.Parse();
			return string.Join(",", sorting.Members.Select(p =>
			{
				var pi = info.PropertyInfoList.FirstOrDefault(i => i.ClassPropertyName == p);
				return string.Format("{0}\"{1}\" {2}", tableEx, pi == null ? p : pi.TableColumnName, sort);
			}));
		}

		internal static string Parse(this Sorting sorting, Dictionary<string, ColumnInfo> columnInfos, bool caseSensitive)
		{
			var sort = sorting.Mode.Parse();
			var format = caseSensitive ? "{0}.\"{1}\" {2}" : "{0}.{1} {2}";

			return string.Join(",", sorting.Members.Select(p =>
			{

				if(!columnInfos.ContainsKey(p))
					throw new Exception(string.Format("未找到属性\"{0}\"的描述信息", p));

				var columnInfo = columnInfos[p];
				var pi = columnInfo.PropertyInfo;

				return string.Format(format, columnInfo.JoinInfo.TableEx, pi == null ? columnInfo.Field : pi.TableColumnName, sort);
			}));
		}

		public static string ParseWhere(this IDictionary<string, object> where, string tableEx, ref int startFormatIndex, out object[] values)
		{
			if(!string.IsNullOrEmpty(tableEx) && !tableEx.EndsWith("."))
				tableEx += ".";

			var list = new List<object>();
			var index = startFormatIndex;
			var wheresql = string.Join(" AND ", where.Where(p => !string.IsNullOrEmpty(p.Key)).Select(p =>
			{
				var str = "";

				if(p.Value == null)
					str = string.Format("{0}\"{1}\" IS NULL", tableEx, p.Key);
				else if(p.Value is DateTime[])
				{
					str = string.Format("{0}\"{1}\" >={{{2}}} AND {0}\"{0}\" < {{{3}}}", tableEx, p.Key, list.Count + index, list.Count + 1 + index);
					var dts = (DateTime[])p.Value;
					list.Add(dts[0]);
					list.Add(dts[1]);
				}
				else if(p.Value is DateTime?[])
				{
					var dts = (DateTime?[])p.Value;
					var and = "";
					if(dts[0].HasValue)
					{
						str = string.Format("{0}\"{1}\" >= {{{2}}}", tableEx, p.Key, list.Count + index);
						list.Add(dts[0]);
						and = " AND ";
					}
					if(dts[1].HasValue)
					{
						str += string.Format("{0}{1}\"{2}\" < {{{3}}} ", and, tableEx, p.Key, list.Count + index);
						list.Add(dts[1]);
					}
				}
				else if(p.Value is Array)
				{
					var array = ((Array)p.Value).Cast<object>();
					str = string.Format("{0}\"{1}\" IN ({2})", tableEx, p.Key, string.Join(",", array.Select((pp, i) => "{" + (list.Count + i + index) + "}")));
					list.AddRange(array);
				}
				else
				{
					str = string.Format("{0}\"{1}\"={{{2}}}", tableEx, p.Key, list.Count + index);
					list.Add(p.Value);
				}
				return str;
			}));

			values = list.ToArray();
			startFormatIndex += values.Length;

			return wheresql;
		}

		public static ICondition ParseClause(this IDictionary<string, object> dic)
		{
			if(dic == null || dic.Count == 0)
				return null;

			var clause = new ConditionCollection(ConditionCombine.And);
			foreach(var key in dic.Keys)
			{
				clause.Add(new Condition(key, dic[key]));
			}
			return clause;
		}
	}
}
