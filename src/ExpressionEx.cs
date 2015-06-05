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
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Automao.Data
{
	public static class ExpressionEx
	{
		public static string ParseOracleSql(this Expression expression, out List<object> paramers)
		{
			return ConvertString(expression, out paramers);
		}

		private static string ConvertString(Expression expression, out List<object> paramers)
		{
			paramers = new List<object>();
			if(expression.CanReduce)
				expression = expression.Reduce();
			if(expression is MethodCallExpression)
			{
				var method = (MethodCallExpression)expression;
				if(method.Method.Name.StartsWith("OrderBy"))
				{
					if(method.Arguments.Count > 1)
						return string.Format("Order By {0} {1}", ConvertString(method.Arguments[1], out paramers), method.Method.Name.EndsWith("OrderByDescending") ? "desc" : "");
				}
				return "";
			}
			else if(expression is LambdaExpression)
			{
				var lambda = (LambdaExpression)expression;
				return ConvertString(lambda.Body, out paramers);
			}
			else if(expression is ParameterExpression)
			{
				var parameter = (ParameterExpression)expression;
				return parameter.Type.Name;
			}
			else if(expression is BinaryExpression)
			{
				var binary = (BinaryExpression)expression;
				return ConvertString(binary.Left, out paramers) + " " + binary.NodeType.ToSQL() + " " + ConvertString(binary.Right, out paramers);
			}
			else if(expression is MemberExpression)
			{
				var member = (MemberExpression)expression;
				if(member.Member.MemberType == System.Reflection.MemberTypes.Field)
				{
					var constant = (ConstantExpression)member.Expression;
					if(constant != null)
					{
						paramers.Add(constant.Value.GetType().GetField(member.Member.Name).GetValue(constant.Value));
						return string.Format("{{{0}}}", paramers.Count - 1);
					}
					return "null";
				}
				return string.Format("{0}.\"{1}\"", ConvertString(member.Expression, out paramers), member.Member.Name);
			}
			else if(expression is ConstantExpression)
			{
				var constant = (ConstantExpression)expression;
				paramers.Add(constant.Value);
				return string.Format("{{{0}}}", paramers.Count - 1);
			}
			else if(expression is NewExpression)
			{
				var newExpression = (NewExpression)expression;
				var list = new List<string>();
				foreach(var item in newExpression.Arguments)
				{
					list.Add(ConvertString(item, out paramers));
				}
				return string.Join(",", list);
			}
			else if(expression is UnaryExpression)
			{
				var newExpression = (UnaryExpression)expression;
				if(newExpression.NodeType == ExpressionType.Convert)
				{
					return string.Format("_{0}.Convert({1})", newExpression.Type.Name, ConvertString(newExpression.Operand, out paramers));
				}
				return "_?";
			}
			else
				return "_" + expression.GetType().FullName;
		}

		public static string[] ResolveExpression<T>(this Expression<Func<T, object>> expression)
		{
			return ResolveExpression(expression.Body, expression.Parameters[0]);
		}

		private static string[] ResolveExpression(Expression expression, ParameterExpression parameter)
		{
			if(expression == parameter)
				return GetMembers(expression.Type);
			if(expression.GetType().FullName == "System.Linq.Expressions.PropertyExpression")
			{
				if(IsScalarType(expression.Type))
					return new string[] { ExpressionToString(expression, parameter) };

				var memberName = GetMemberName(expression, parameter);
				return GetMembers(expression.Type, memberName);
			}
			if(expression is NewExpression)
			{
				HashSet<string> list = new HashSet<string>();
				var ne = (NewExpression)expression;
				foreach(var argument in ne.Arguments)
				{
					list.UnionWith(ResolveExpression(argument, parameter));
				}
				return list.ToArray();
			}

			if(expression.NodeType == ExpressionType.Convert && expression.Type == typeof(object))
				return ResolveExpression(((UnaryExpression)expression).Operand, parameter);

			throw new NotSupportedException();
		}

		private static string ExpressionToString(Expression expression, Expression stop)
		{
			if(expression == stop)
				return string.Empty;

			dynamic propertyExpression = expression;

			var str = ExpressionToString(propertyExpression.Expression, stop);

			if(string.IsNullOrEmpty(str))
				return propertyExpression.Member.Name;
			return str + "." + propertyExpression.Member.Name;
		}

		private static string GetMemberName(Expression expression, ParameterExpression parameter)
		{
			dynamic propertyExpression = expression;

			var temp = (Expression)propertyExpression.Expression;

			if(temp == parameter)
				return propertyExpression.Member.Name;

			return GetMemberName(temp, parameter) + "." + propertyExpression.Member.Name;
		}

		private static string[] GetMembers(Type type, string prev = null)
		{
			if(prev == null)
				prev = string.Empty;
			else
				prev += ".";

			return type.GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(p => IsScalarType(p.PropertyType)).Select(p => prev + p.Name).ToArray();
		}

		private static bool IsScalarType(Type type)
		{
			if(type.IsArray)
				return IsScalarType(type.GetElementType());

			if(type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
				return IsScalarType(type.GetGenericArguments()[0]);

			return type.IsPrimitive || type.IsEnum ||
				   type == typeof(string) || type == typeof(DateTime) ||
				   type == typeof(Guid) || type == typeof(TimeSpan);
		}
	}

	public static class ExpressionTypeEx
	{
		/// <summary>
		/// 把操作符转换成加减乘除什么的。
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static string ToSQL(this ExpressionType type)
		{
			switch(type)
			{
				case ExpressionType.Add:
					return "+";
				case ExpressionType.AndAlso:
					return "and";
				case ExpressionType.Equal:
					return "=";
				case ExpressionType.GreaterThan:
					return ">";
				case ExpressionType.GreaterThanOrEqual:
					return ">=";
				case ExpressionType.LessThan:
					return "<";
				case ExpressionType.LessThanOrEqual:
					return "<=";
				case ExpressionType.Multiply:
					return "*";
				case ExpressionType.NotEqual:
					return "is not";
				case ExpressionType.OrElse:
					return "or";
				case ExpressionType.Subtract:
					return "-";
				case ExpressionType.Divide:
					return "/";
				default:
					throw new ArgumentOutOfRangeException("type", type.ToString());
			}
		}
	}
}
