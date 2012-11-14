#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/IO/WriteBufferFromString.h>
#include <statdaemons/Stopwatch.h>
#include <string>
#include <streambuf>
#include <iostream>
#include <cstdio>

int main(int argc, char ** argv)
{
	try
	{
		int repeats = 1;
		if (argc >= 2)
			repeats = atoi(argv[1]);
		
		std::string text((std::istreambuf_iterator<char>(std::cin)),
		                  std::istreambuf_iterator<char>());
		
		std::cout << "Text length: " << text.size() << std::endl;
		
		Stopwatch timer;
		std::string str1;
		{
			DB::WriteBufferFromString simple_buf(str1);
			for (int i = 0; i < repeats; ++i)
			{
				simple_buf.write(text.data(), text.size());
			}
		}
		std::cout << "Wrote to string in " << timer.elapsedSeconds() << "s." << std::endl;
		std::cout << "String length: " << str1.size() << "(" << (str1.size() == text.size() * repeats ? "as " : "un") << "expected)" << std::endl;
		
		timer.restart();
		
		std::string str2;
		{
			DB::WriteBufferFromString simple_buf(str1);
			DB::WriteBufferValidUTF8 utf_buf(simple_buf);
			for (int i = 0; i < repeats; ++i)
			{
				utf_buf.write(text.data(), text.size());
			}
		}
		std::cout << "Wrote to UTF8 in " << timer.elapsedSeconds() << "s." << std::endl;
		std::cout << "String length: " << str2.size() << "(" << (str2.size() == text.size() * repeats ? "as " : "un") << "expected)" << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
