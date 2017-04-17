using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Automao.Data.Mapping;

namespace Automao.Data.MySql
{
	public class MySqlClassInfo : ClassInfo
	{
		public MySqlClassInfo(string @as, ClassNode classNode)
			: base(@as, classNode)
		{
		}

		protected override string GetTableName(ClassNode classNode, string asName)
		{
			return string.Format("{0} {1}", classNode.GetTableName(), asName);
		}
	}
}
