using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automao.Data.MySql
{
	public class MySqlColumnInfo : ColumnInfo
	{
		#region 构造函数
		public MySqlColumnInfo(string original)
			: base(original)
		{
		}
		#endregion

		#region override
		protected override string ToColumn(string asName, string field)
		{
			return string.Format("{0}`{1}`", asName, field);
		}

		protected override string ToColumn(string aggregateFunctionName, string asName, string field)
		{
			return string.Format("{0}({1}`{2}`)", aggregateFunctionName, asName, field);
		}

		protected override string GetColumnEx(string asName, string field)
		{
			return string.Format("{0}_{1}", asName, field);
		}

		protected override string GetColumnEx(string aggregateFunctionName, string asName, string field)
		{
			return string.Format("{1}_{0}_{2}", aggregateFunctionName, asName, field);
		}
		#endregion

		#region 静态方法
		public static Dictionary<string, ColumnInfo> Create(IEnumerable<string> columns, ClassInfo root)
		{
			return ColumnInfo.Create(columns, root, p => new MySqlColumnInfo(p), (@as, classNode) => new MySqlClassInfo(@as, classNode));
		}
		#endregion
	}
}
