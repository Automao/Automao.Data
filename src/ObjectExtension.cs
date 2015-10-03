using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automao.Data
{
	public static class ObjectExtension
	{
		/// <summary>
		/// 判断类型是否是字典类型
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static bool IsDictionary(this Type type)
		{
			if(type.IsSubclassOf(typeof(Dictionary<string, object>)))
				return false;

			return type == typeof(Dictionary<string, object>)
				|| type == typeof(IDictionary<string, object>)
				|| type == typeof(IDictionary)
				|| type.GetInterface(typeof(IDictionary<string, object>).Name) != null
				|| type.GetInterface(typeof(IDictionary).Name) != null;
		}
	}
}
