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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Automao.Data.Mapping;
using Zongsoft.Data;

namespace Automao.Data
{
	public static class ObjectAccessExtension
	{
		internal static string ToWhere(this ICondition condition, Dictionary<string, ColumnInfo> columnInfos, bool caseSensitive, ref int tableIndex, ref int joinStartIndex, ref int valueIndex, out object[] values, string format = "WHERE {0}")
		{
			var where = Resolve(condition, columnInfos, caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
			if(string.IsNullOrEmpty(where))
				return "";
			return string.Format(format, where);
		}

		private static string Resolve(ICondition condition, Dictionary<string, ColumnInfo> columnInfos, bool caseSensitive, ref int tableIndex, ref int joinStartIndex, ref int valueIndex, out object[] values)
		{
			if(condition is Condition)
			{
				var where = (Condition)condition;
				values = GetValue(where);

				if(!columnInfos.ContainsKey(where.Name))
					throw new Exception(string.Format("未找到属性\"{0}\"的描述信息", where.Name));

				var columnInfo = columnInfos[where.Name];

				var oper = where.Operator.Parse(ref values, ref tableIndex, ref joinStartIndex, ref valueIndex);

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
					var result = Resolve(item, columnInfos, caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
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

				return string.Join(where.ConditionCombine.Parse(), sqls);
			}

			values = new object[0];
			return null;
		}

		private static string Parse(this ConditionOperator clauseOperator, ref object[] values, ref int tableIndex, ref int joinStartIndex, ref int valueIndex)
		{
			switch(clauseOperator)
			{
				case ConditionOperator.Between:
					return string.Format("BETWEEN {{{0}}} AND {{{1}}}", valueIndex++, valueIndex++);
				case ConditionOperator.Equal:
				case ConditionOperator.Like:
					{
						if(values == null || values.Length == 0)
							return "IS NULL";
						return string.Format("{0} {{{1}}}", values[0] is string && ((string)values[0]).IndexOfAny("_%".ToArray()) >= 0 ? "LIKE" : "=", valueIndex++);
					}
				case ConditionOperator.GreaterThan:
					return string.Format("> {{{0}}}", valueIndex++);
				case ConditionOperator.GreaterThanEqual:
					return string.Format(">= {{{0}}}", valueIndex++);
				case ConditionOperator.In:
				case ConditionOperator.NotIn:
					{
						if(values == null || values.Length == 0)
							return "";

						if(values.Length == 1)
						{
							var value = values[0];
							if(value is ObjectAccessResult)
							{
								var objectAccessResult = (ObjectAccessResult)value;
								var sql = objectAccessResult.CreateSql(ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
								return string.Format("{0} ({1})", clauseOperator == ConditionOperator.NotIn ? "NOT IN" : "IN", sql);
							}
							return string.Format("{0} {{{1}}}", clauseOperator == ConditionOperator.NotIn ? "!=" : "=", valueIndex++);
						}

						var list = new List<string>();
						for(int i = 0; i < values.Length; i++)
						{
							list.Add(string.Format("{{{0}}}", valueIndex + i));
						}

						valueIndex += values.Length;
						return string.Format("{0} ({1})", clauseOperator == ConditionOperator.NotIn ? "NOT IN" : "IN", string.Join(",", list));
					}

				case ConditionOperator.LessThan:
					return string.Format("< {{{0}}}", valueIndex++);
				case ConditionOperator.LessThanEqual:
					return string.Format("<= {{{0}}}", valueIndex++);
				case ConditionOperator.NotEqual:
					{
						if(values == null || values.Length == 0)
							return "IS NOT NULL";

						return string.Format("!= {{{0}}}", valueIndex++);
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

		public static string Parse(this Sorting sorting, ClassNode info, string tableEx)
		{
			if(!string.IsNullOrEmpty(tableEx))
				tableEx += ".";

			var sort = sorting.Mode.Parse();
			return string.Join(",", sorting.Members.Select(p =>
			{
				var pi = info.PropertyNodeList.FirstOrDefault(i => i.Name == p);
				return string.Format("{0}\"{1}\" {2}", tableEx, pi == null ? p : pi.Column, sort);
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
				var pi = columnInfo.PropertyNode;

				return string.Format(format, columnInfo.ClassInfo.AsName, pi == null ? columnInfo.Field : pi.Column, sort);
			}));
		}

		private static object[] GetValue(Condition where)
		{
			if(where.Value == null)
				return new object[0];
			else if(where.Value is string || where.Value is byte[] || where.Value is ObjectAccessResult)
				return new[] { where.Value };
			else if(where.Value is object[])
				return (object[])where.Value;
			else if(where.Value is IEnumerable<object>)
				return ((IEnumerable<object>)where.Value).ToArray();
			else if(where.Value is IEnumerable)
				return ((IEnumerable)where.Value).Cast<object>().ToArray();
			else
				return new[] { where.Value };
		}
	}
}
