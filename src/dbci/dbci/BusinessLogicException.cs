using System;
using System.Collections.Generic;
using System.Text;

namespace dbci
{
    public class BusinessLogicException : Exception
    {
        public BusinessLogicException(string message) : base(message) { 
        }
    }
}
