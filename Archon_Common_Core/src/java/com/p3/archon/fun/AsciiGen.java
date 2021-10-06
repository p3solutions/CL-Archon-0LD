package com.p3.archon.fun;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

public class AsciiGen {

	private BufferedImage img;
	private double pixval;
	private PrintWriter prntwrt;
	private FileWriter filewrt;

	public AsciiGen() {
		prntwrt = new PrintWriter(System.out);
	}

	public void convertToAscii(InputStream imgname) throws IOException {
		img = ImageIO.read(imgname);

		for (int i = 0; i < img.getHeight(); i++) {
			for (int j = 0; j < img.getWidth(); j++) {
				Color pixcol = new Color(img.getRGB(j, i));
				pixval = (((pixcol.getRed() * 0.30) + (pixcol.getBlue() * 0.59) + (pixcol.getGreen() * 0.11)));
				print(strChar(pixval));
			}
			try {
				prntwrt.println("");
				prntwrt.flush();
				filewrt.flush();
			} catch (Exception ex) {
			}
		}
	}

	public String strChar(double g) {
		String str = " ";
		if (g >= 240) {
			str = " ";
		} else if (g >= 210) {
			str = ".";
		} else if (g >= 190) {
			str = "*";
		} else if (g >= 170) {
			str = "+";
		} else if (g >= 120) {
			str = "^";
		} else if (g >= 110) {
			str = "&";
		} else if (g >= 80) {
			str = "8";
		} else if (g >= 60) {
			str = "#";
		} else {
			str = "@";
		}
		return str;
	}

	public void print(String str) {
		try {
			prntwrt.print(str);
			prntwrt.flush();
			filewrt.flush();
		} catch (Exception ex) {
		}
	}

	public static void start() {
		try {
			AsciiGen obj = new AsciiGen();
			obj.convertToAscii(AsciiGen.class.getResourceAsStream("/img/archon-small.jpg"));
		} catch (Exception e) {
			System.out.println("-----------------------------");
			System.out.println("Platform 3 Solutions - Archon");
			System.out.println("-----------------------------");
		}
	}
	
}
