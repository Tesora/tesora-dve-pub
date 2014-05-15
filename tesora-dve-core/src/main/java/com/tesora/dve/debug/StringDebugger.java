// OS_STATUS: public
package com.tesora.dve.debug;

import java.io.IOException;


public class StringDebugger implements DebugHandle, Debuggable {
    String fieldStart;
    String subIndent;
    String fieldEnd;
    String nameValueDelimeter;
    StringBuilder builder;

    public StringDebugger() {
        this("","\t","\n"," = ",new StringBuilder());
    }

    public StringDebugger(String fieldStart, String subIndent, String fieldEnd, String nameValueDelimeter, StringBuilder builder) {
        this.fieldStart = fieldStart;
        this.subIndent = subIndent;
        this.fieldEnd = fieldEnd;
        this.nameValueDelimeter = nameValueDelimeter;
        this.builder = builder;
    }

    @Override
    public StringDebugger entry(String name, Object value) {
        if (value instanceof Debuggable){
            return this.entry(name, (Debuggable) value);
        }

        builder.append(fieldStart);
        builder.append(name);
        builder.append(nameValueDelimeter);
        builder.append(value);
        builder.append(fieldEnd);
        return this;
    }

    @Override
    public StringDebugger line(String text) {
        builder.append(fieldStart);
        builder.append(text);
        builder.append(fieldEnd);
        return this;
    }

    public StringDebugger nesting(){
        return new StringDebugger(fieldStart + subIndent, subIndent, fieldEnd, nameValueDelimeter,builder);
    }

    @Override
    public StringDebugger entry(String name, Debuggable value) {
        entry(name, "[");
        if (value == null)
            builder.append("null");
        else {
            value.writeTo(this.nesting());
        }
        line("]");
        return this;
    }

    public String toString(){
        return builder.toString();
    }

    @Override
    public void writeTo(DebugHandle display) {
        display.line(builder.toString());
    }


    public static void output(Appendable appender, Debuggable debug)  {
        StringDebugger buffer = new StringDebugger();
        debug.writeTo(buffer);
        try {
            appender.append(buffer.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void output(Appendable appender, String named, Object debug)  {
        StringDebugger buffer = new StringDebugger();
        buffer.entry(named,debug);
        try {
            appender.append(buffer.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
