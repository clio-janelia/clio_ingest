SCRIPT="""
/**
 * Align two images pre.png and post.png using SIFT for both translation and affine.
 * Export success or failure into SIFT.txt, shutdown.
 *
 * Start this script in headless fiji, e.g. on 64bit Linux:
 * 
 * ./fiji -Dpre="./pre.png" -Dpost="./post.png" -- --headless "fiji_align.bsh"
 *
 * @author Stephan Saalfeld <saalfeld@mpi-cbg.de> 2012-08-24
 *                                     modified   2013-06-05
 *                                                2014-04-21
 *
 *	swapped order of pre and post images so that the transformation matrix is to be applied to the post image for matching the pre image
 *						  2016-11-17
 *					small modifications to output (Stephen Plaza)
					          2020-04-10	
 */

import java.io.*;
import java.util.*;

import ij.ImagePlus;

import mpicbg.ij.FeatureTransform;
import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.models.AffineModel2D;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.NoninvertibleModelException;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import org.json.simple.JSONObject;

FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param();

imgPathPre = System.getProperty( "pre", "./pre.png" );
imgPathPost = System.getProperty( "post", "./post.png" );

/* custom parameters */
p.fdSize = 4;
p.maxOctaveSize = 2048;
p.minOctaveSize = 64;
maxSteps = 5;

float rod = 0.92f;
float maxEpsilon = 25f;
float minInlierRatio = 0.05f;
int minNumInliers = 20;

/* images */
imp1 = new ImagePlus( imgPathPost);
ij.IJ.run(imp1,"Enhance Contrast", "saturated=0.35");
imp2 = new ImagePlus( imgPathPre );
ij.IJ.run(imp2,"Enhance Contrast", "saturated=0.35");

/* transformation models  */
AffineModel2D affine = new AffineModel2D();
TranslationModel2D translation = new TranslationModel2D();

/* matching success */
boolean affineFound = false;
boolean translationFound = false;

/* match */
void match() {
	FloatArray2DSIFT sift = new FloatArray2DSIFT( p );
	SIFT ijSIFT = new SIFT( sift );
	ArrayList features1 = new ArrayList();
	ArrayList features2 = new ArrayList();
	ArrayList candidates = new ArrayList();
	ArrayList inliers = new ArrayList();
		
	ijSIFT.extractFeatures( imp1.getProcessor(), features1 );
	ijSIFT.extractFeatures( imp2.getProcessor(), features2 );

	if ( features1.size() > 0 && features2.size() > 0 )
		FeatureTransform.matchFeatures( features1, features2, candidates, rod );

	try {
		affineFound = affine.filterRansac(
			candidates,
			inliers,
			1000,
			maxEpsilon,
			minInlierRatio,
			minNumInliers,
			3 );
	}
	catch ( NotEnoughDataPointsException e ) {
		affineFound = false;
	}

	try {
		translationFound = translation.filterRansac(
			candidates,
			inliers,
			1000,
			maxEpsilon,
			minInlierRatio,
			minNumInliers,
			3 );
	}
	catch ( NotEnoughDataPointsException e ) {
		translationFound = false;
	}
}

/* main */
do {
	//System.out.println( p.steps );
	match();
	++p.steps;
} while (p.steps < maxSteps + 1 && !(affineFound && translationFound));

/* try hard!!! */
if (!(affineFound && translationFound)) {
	p.steps = maxSteps;
	p.initialSigma = 0.8f;
	match();
}

/* export results */
try {
	System.out.println("{");
	System.out.println("\\"width\\":" + imp1.getWidth().toString() + ",");
	System.out.println("\\"height\\":" + imp1.getHeight().toString() + ",");
	
	/*JSONObject obj = new JSONObject();

	if ( affineFound )
		obj.put("Affine", affine.toArray().toString()); 
	
	if ( translationFound )
		obj.put("Translation", translation.toArray().toString()); 
					
	
	System.out.println(obj.toJSONString());*/
	
	if ( affineFound ) {
		double[] affine_arr = new double[6];
		affine.toArray(affine_arr);
		System.out.println("\\"affine\\":" + Arrays.toString(affine_arr) + ",");
	} else {
		System.out.println("\\"affine\\": [1, 0, 0, 0, 1, 0],");
	}

	if ( translationFound ) {
		double[] trans_arr = new double[6];
		translation.toArray(trans_arr);
		System.out.println("\\"translation\\":" + Arrays.toString(trans_arr));
	} else {
		System.out.println("\\"translation\\": [1, 0, 0, 0, 1, 0],");
	}
	System.out.println("}");
}
catch ( e ) {
	e.printStackTrace();
}

/* shutdown */
java.lang.Runtime.getRuntime().exit( 0 );
"""
